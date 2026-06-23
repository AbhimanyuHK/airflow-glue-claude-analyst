"""
aws_logs.py — AWS CloudWatch log fetcher for Airflow (MWAA) and AWS Glue jobs.

Supports:
  - MWAA task-level log streams
  - Self-hosted Airflow (configurable log group)
  - Glue error + output log groups
  - Automatic stream discovery when exact stream name is unknown
  - Pagination for large log streams
  - Exponential backoff retry on throttling
"""

import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class AWSLogFetcher:
    """Fetches CloudWatch log events for a given Airflow task or Glue job run."""

    # How many log events to pull per page (AWS max is 10 000)
    _PAGE_SIZE = 500
    # Max pages to fetch (to avoid runaway loops on huge streams)
    _MAX_PAGES = 6
    # Retry settings
    _RETRY_ATTEMPTS = 3
    _RETRY_BACKOFF = 1.5  # seconds (doubles on each retry)

    def __init__(self) -> None:
        self._client = boto3.client(
            "logs",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
        )
        self.lookback_hours: int = int(os.environ.get("LOG_LOOKBACK_HOURS", 2))
        self.max_log_chars: int = int(os.environ.get("MAX_LOG_CHARS", 20_000))

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def fetch_logs(self, job_info: dict) -> Optional[str]:
        """
        Route to the appropriate fetcher based on job_info['source'].

        Returns a plain-text string of log lines, or None if nothing was found.
        """
        source = job_info.get("source")
        if source == "airflow":
            return self._fetch_airflow_logs(job_info)
        if source == "glue":
            return self._fetch_glue_logs(job_info)
        logger.warning("Unknown source '%s' — cannot fetch logs.", source)
        return None

    # -----------------------------------------------------------------------
    # Airflow / MWAA
    # -----------------------------------------------------------------------

    def _fetch_airflow_logs(self, job_info: dict) -> Optional[str]:
        """
        Fetch task logs from MWAA or self-hosted Airflow on CloudWatch.

        MWAA log stream naming convention:
          task/<dag_id>/<run_id>/<task_id>/attempt=1.log
        """
        log_group = job_info.get("log_group", "/aws/mwaa/airflow")
        dag_id = job_info.get("dag_id", "")
        task_id = job_info.get("task_id", "")
        run_id = job_info.get("run_id", "")

        # Build increasingly broad prefixes so we always find *something*
        prefixes = []
        if dag_id and run_id and task_id:
            prefixes.append(f"task/{dag_id}/{run_id}/{task_id}")
        if dag_id and run_id:
            prefixes.append(f"task/{dag_id}/{run_id}")
        if dag_id:
            prefixes.append(f"task/{dag_id}")

        for prefix in prefixes:
            logger.info("Searching Airflow streams: group=%s prefix=%s", log_group, prefix)
            streams = self._find_streams(log_group, prefix, max_results=5)
            if streams:
                # Pick the most recently written stream
                stream_name = streams[0]["logStreamName"]
                logs = self._read_stream(log_group, stream_name)
                if logs:
                    return logs

        logger.warning("No Airflow log streams found for dag_id=%s", dag_id)
        return None

    # -----------------------------------------------------------------------
    # AWS Glue
    # -----------------------------------------------------------------------

    def _fetch_glue_logs(self, job_info: dict) -> Optional[str]:
        """
        Fetch Glue job logs from both the error and output log groups.

        Glue writes to two groups:
          /aws-glue/jobs/error  — exceptions, stderr
          /aws-glue/jobs/output — stdout / driver logs
        Stream name: <job_name>/<run_id>
        """
        job_name = job_info.get("job_name", "")
        run_id = job_info.get("run_id", "")

        error_group = os.environ.get("GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error")
        output_group = os.environ.get("GLUE_OUTPUT_LOG_GROUP", "/aws-glue/jobs/output")

        sections: list[str] = []

        for group_label, log_group in [("ERROR", error_group), ("OUTPUT", output_group)]:
            logs = self._fetch_glue_group(log_group, job_name, run_id)
            if logs:
                sections.append(f"=== Glue {group_label} logs ({log_group}) ===\n{logs}")

        return "\n\n".join(sections) if sections else None

    def _fetch_glue_group(
        self, log_group: str, job_name: str, run_id: str
    ) -> Optional[str]:
        """Try to read a specific Glue stream, falling back to prefix search."""
        # Exact stream first
        if job_name and run_id:
            exact_stream = f"{job_name}/{run_id}"
            logs = self._read_stream(log_group, exact_stream)
            if logs:
                return logs

        # Prefix fallback
        prefix = job_name or run_id
        if prefix:
            streams = self._find_streams(log_group, prefix, max_results=3)
            for stream in streams:
                logs = self._read_stream(log_group, stream["logStreamName"])
                if logs:
                    return logs

        return None

    # -----------------------------------------------------------------------
    # CloudWatch helpers
    # -----------------------------------------------------------------------

    def _find_streams(
        self, log_group: str, prefix: str, max_results: int = 5
    ) -> list[dict]:
        """
        Return up to *max_results* log streams whose names start with *prefix*,
        ordered by LastEventTime descending (most recent first).
        """
        try:
            resp = self._call_with_retry(
                self._client.describe_log_streams,
                logGroupName=log_group,
                logStreamNamePrefix=prefix,
                orderBy="LastEventTime",
                descending=True,
                limit=max_results,
            )
            return resp.get("logStreams", [])
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code == "ResourceNotFoundException":
                logger.warning("Log group not found: %s", log_group)
            else:
                logger.error("describe_log_streams error (%s): %s", code, exc)
            return []

    def _read_stream(self, log_group: str, stream_name: str) -> Optional[str]:
        """
        Pull log events from *stream_name* within the lookback window.
        Pages through results up to _MAX_PAGES, then truncates intelligently.
        Returns a plain-text string of timestamped log lines, or None.
        """
        start_ms = int(
            (datetime.now(timezone.utc) - timedelta(hours=self.lookback_hours)).timestamp()
            * 1000
        )

        lines: list[str] = []
        next_token: Optional[str] = None
        pages_fetched = 0

        while pages_fetched < self._MAX_PAGES:
            kwargs: dict = {
                "logGroupName": log_group,
                "logStreamName": stream_name,
                "startTime": start_ms,
                "startFromHead": True,
                "limit": self._PAGE_SIZE,
            }
            if next_token:
                kwargs["nextForwardToken"] = next_token

            try:
                resp = self._call_with_retry(self._client.get_log_events, **kwargs)
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code == "ResourceNotFoundException":
                    logger.warning("Stream not found: %s/%s", log_group, stream_name)
                else:
                    logger.error("get_log_events error (%s): %s", code, exc)
                break

            events = resp.get("events", [])
            if not events:
                break

            for ev in events:
                ts = datetime.fromtimestamp(
                    ev["timestamp"] / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                lines.append(f"[{ts}] {ev['message'].rstrip()}")

            new_token = resp.get("nextForwardToken")
            if new_token == next_token:
                # No more pages
                break
            next_token = new_token
            pages_fetched += 1

        if not lines:
            return None

        full_text = "\n".join(lines)
        logger.info(
            "Fetched %d lines (%d chars) from %s/%s",
            len(lines),
            len(full_text),
            log_group,
            stream_name,
        )
        return self._smart_truncate(full_text)

    def _smart_truncate(self, text: str) -> str:
        """
        If the log exceeds max_log_chars, keep the first 10 % (context)
        and the last 90 % (where errors usually appear).
        """
        if len(text) <= self.max_log_chars:
            return text

        head_size = max(500, self.max_log_chars // 10)
        tail_size = self.max_log_chars - head_size - 60

        head = text[:head_size]
        tail = text[-tail_size:]
        omitted = len(text) - head_size - tail_size

        logger.info("Truncated log from %d to %d chars (%d omitted)", len(text), self.max_log_chars, omitted)
        return f"{head}\n\n... [{omitted:,} characters omitted] ...\n\n{tail}"

    def _call_with_retry(self, fn, **kwargs):
        """Call a boto3 function with exponential backoff on ThrottlingException."""
        delay = self._RETRY_BACKOFF
        for attempt in range(1, self._RETRY_ATTEMPTS + 1):
            try:
                return fn(**kwargs)
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                if code in ("ThrottlingException", "ServiceUnavailableException") and attempt < self._RETRY_ATTEMPTS:
                    logger.warning(
                        "AWS throttle on attempt %d/%d — retrying in %.1fs",
                        attempt, self._RETRY_ATTEMPTS, delay,
                    )
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise
