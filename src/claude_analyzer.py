"""
claude_analyzer.py — Root cause analysis via the Anthropic Claude API.

Sends job metadata + CloudWatch logs to Claude and parses the structured
JSON response into a dict consumed by the Slack Block Kit formatter.
"""

import os
import json
import logging

import anthropic

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an expert data engineering SRE (Site Reliability Engineer) with deep \
knowledge of Apache Airflow, AWS Glue, Python, PySpark, and AWS infrastructure.

Your job: analyze CloudWatch logs from a failed data pipeline and produce a \
clear, actionable root cause analysis.

RESPONSE FORMAT — reply with ONLY a valid JSON object, no markdown fences, \
no preamble, no trailing text:

{
  "root_cause": "<one sentence — the exact thing that failed, e.g. 'S3 key not found: s3://bucket/path/file.parquet'>",
  "summary": "<2-3 sentences explaining what happened and why>",
  "fix": "<numbered list of concrete steps to resolve the issue, one per line, e.g. '1. ...\\n2. ...\\n3. ...'>",
  "affected_components": "<comma-separated list of DAG tasks, Glue job stages, or AWS resources affected>",
  "error_snippet": "<the 3-8 most diagnostic log lines that show the root error, verbatim>",
  "severity": "<one of: critical | high | medium | low>",
  "category": "<one of: dependency_failure | resource_exhaustion | permission_error | data_quality | timeout | code_error | configuration | network | unknown>"
}

RULES:
- Be specific — reference actual values from the logs (table names, file paths, error codes, line numbers).
- Do NOT be vague. "An error occurred" is not a root cause.
- For the fix, provide actionable steps an engineer can execute immediately.
- severity=critical means data loss or production outage; high means pipeline blocked; medium/low means degraded.
- If the logs are insufficient to determine the root cause, set category="unknown" and explain what additional information is needed in the summary.
"""

# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------


class ClaudeAnalyzer:
    """Wraps the Anthropic API to produce structured root cause analysis."""

    def __init__(self) -> None:
        self._client = anthropic.Anthropic(
            api_key=os.environ["ANTHROPIC_API_KEY"],
        )
        self.model: str = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6")
        self.max_tokens: int = int(os.environ.get("CLAUDE_MAX_TOKENS", 1500))

    # -----------------------------------------------------------------------
    # Public interface
    # -----------------------------------------------------------------------

    def analyze(
        self,
        source: str,
        job_info: dict,
        original_alert: str,
        logs: str,
    ) -> dict:
        """
        Send the error context + logs to Claude and return a structured dict.

        Falls back to a safe error dict if the API call or JSON parse fails.
        """
        prompt = self._build_prompt(source, job_info, original_alert, logs)

        logger.info(
            "Sending %d chars to Claude (%s) for analysis", len(prompt), self.model
        )

        try:
            response = self._client.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIStatusError as exc:
            logger.error("Anthropic API error %s: %s", exc.status_code, exc.message)
            raise

        raw: str = response.content[0].text.strip()
        logger.debug("Claude raw response: %s", raw[:500])

        return self._parse_response(raw, job_info)

    # -----------------------------------------------------------------------
    # Prompt builder
    # -----------------------------------------------------------------------

    def _build_prompt(
        self,
        source: str,
        job_info: dict,
        original_alert: str,
        logs: str,
    ) -> str:
        if source == "airflow":
            job_context = (
                "JOB TYPE    : Apache Airflow (MWAA)\n"
                f"DAG ID      : {job_info.get('dag_id', 'unknown')}\n"
                f"Task ID     : {job_info.get('task_id', 'unknown')}\n"
                f"Run ID      : {job_info.get('run_id', 'unknown')}\n"
                f"Log group   : {job_info.get('log_group', 'unknown')}\n"
            )
        else:
            job_context = (
                "JOB TYPE    : AWS Glue ETL\n"
                f"Job name    : {job_info.get('job_name', 'unknown')}\n"
                f"Run ID      : {job_info.get('run_id', 'unknown')}\n"
                f"Log group   : {job_info.get('log_group', 'unknown')}\n"
            )

        return (
            "Analyze the following data pipeline failure.\n\n"
            "--- ORIGINAL SLACK ALERT ---\n"
            f"{original_alert}\n\n"
            "--- JOB CONTEXT ---\n"
            f"{job_context}\n"
            "--- CLOUDWATCH LOGS ---\n"
            f"{logs}\n\n"
            "Provide a structured root cause analysis in the JSON format specified."
        )

    # -----------------------------------------------------------------------
    # Response parser
    # -----------------------------------------------------------------------

    def _parse_response(self, raw: str, job_info: dict) -> dict:
        """
        Parse Claude's JSON response. If parsing fails, return a safe fallback
        dict that surfaces the raw text as the summary.
        """
        # Strip accidental markdown fences (shouldn't happen, but defensive)
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[1:])
        if cleaned.endswith("```"):
            cleaned = "\n".join(cleaned.split("\n")[:-1])
        cleaned = cleaned.strip()

        try:
            data = json.loads(cleaned)
            # Ensure all expected keys are present
            defaults = {
                "root_cause": "See summary",
                "summary": "",
                "fix": "Review logs manually.",
                "affected_components": job_info.get("dag_id") or job_info.get("job_name", ""),
                "error_snippet": "",
                "severity": "high",
                "category": "unknown",
            }
            return {**defaults, **data}
        except json.JSONDecodeError:
            logger.warning("Could not parse Claude response as JSON — using raw text.")
            return {
                "root_cause": "Analysis complete — see summary below",
                "summary": raw,
                "fix": "Review the log snippet above and the full CloudWatch stream.",
                "affected_components": job_info.get("dag_id") or job_info.get("job_name", ""),
                "error_snippet": "",
                "severity": "high",
                "category": "unknown",
            }
