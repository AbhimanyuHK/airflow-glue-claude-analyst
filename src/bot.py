"""
bot.py — Slack event handlers, message parsing, and Block Kit formatting.

Flow:
  1. Slack posts an Airflow/Glue error notification to a channel.
  2. An engineer replies "@bot analyze" in that thread.
  3. This module catches the app_mention event, orchestrates log fetching
     and Claude analysis, then posts a rich reply back into the same thread.
"""

import os
import re
import logging
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .aws_logs import AWSLogFetcher
from .claude_analyzer import ClaudeAnalyzer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# App initialisation — lazy so pytest imports don't trigger token validation
# ---------------------------------------------------------------------------

_app: App | None = None
_log_fetcher: AWSLogFetcher | None = None
_analyzer: ClaudeAnalyzer | None = None


def create_app() -> App:
    """
    Return the configured Slack Bolt app, initialising it on first call.
    Keeping initialisation lazy means importing this module in tests never
    touches the Slack API (no token validation at import time).
    """
    global _app, _log_fetcher, _analyzer
    if _app is None:
        _app = App(token=os.environ["SLACK_BOT_TOKEN"])
        _log_fetcher = AWSLogFetcher()
        _analyzer = ClaudeAnalyzer()
        _register_handlers(_app)
    return _app


def start() -> None:
    """Start the bot using Socket Mode (no public URL required)."""
    app = create_app()
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    logger.info("⚡️  Airflow Glue Claude Analyst is running!")
    handler.start()


# ---------------------------------------------------------------------------
# Event handler registration (called once inside create_app)
# ---------------------------------------------------------------------------

def _register_handlers(app: App) -> None:
    """Attach all Slack event listeners to the app instance."""

    @app.event("app_mention")
    def handle_analyze_mention(event: dict, say, client) -> None:
        """
        Fires when someone mentions the bot in any channel it's a member of.
        Only acts when the mention contains the word "analyze".
        """
        text = event.get("text", "").lower()
        if "analyze" not in text:
            return

        channel_id: str = event["channel"]
        thread_ts: str = event.get("thread_ts") or event["ts"]
        user_id: str = event["user"]

        logger.info("Analyze request from user=%s thread=%s", user_id, thread_ts)

        # ── 1. Acknowledge immediately so the user knows the bot is working ──
        say(
            text="🔍 On it! Fetching logs and running analysis — give me a moment.",
            thread_ts=thread_ts,
            channel=channel_id,
        )

        try:
            # ── 2. Fetch the original (root) message of the thread ──────────
            result = client.conversations_replies(channel=channel_id, ts=thread_ts)
            messages = result.get("messages", [])
            if not messages:
                say(
                    text="⚠️ Could not read the thread. Make sure I have `channels:history` scope.",
                    thread_ts=thread_ts,
                    channel=channel_id,
                )
                return

            original_text: str = messages[0].get("text", "")
            attachments = messages[0].get("attachments", [])
            # Some Airflow integrations put details in attachments
            for att in attachments:
                original_text += "\n" + att.get("text", "") + att.get("fallback", "")

            # ── 3. Parse job metadata from the alert message ─────────────────
            job_info = extract_job_info(original_text)

            if not job_info["source"]:
                say(
                    text=(
                        "⚠️ Could not identify this as an Airflow or Glue error.\n"
                        "Make sure the original alert includes the DAG/job name and run ID."
                    ),
                    thread_ts=thread_ts,
                    channel=channel_id,
                )
                return

            source_label = job_info["source"].upper()
            say(
                text=f"📡 Detected *{source_label}* error. Pulling CloudWatch logs…",
                thread_ts=thread_ts,
                channel=channel_id,
            )

            # ── 4. Fetch logs from AWS CloudWatch ────────────────────────────
            logs = _log_fetcher.fetch_logs(job_info)

            if not logs:
                say(
                    text=(
                        "⚠️ Could not retrieve logs from CloudWatch.\n"
                        "Check that the bot's IAM role has `logs:DescribeLogStreams` "
                        "and `logs:GetLogEvents` permissions, and that the log group "
                        f"`{job_info.get('log_group')}` exists."
                    ),
                    thread_ts=thread_ts,
                    channel=channel_id,
                )
                return

            say(
                text="🤖 Logs retrieved! Running Claude AI root cause analysis…",
                thread_ts=thread_ts,
                channel=channel_id,
            )

            # ── 5. Claude analysis ───────────────────────────────────────────
            analysis = _analyzer.analyze(
                source=job_info["source"],
                job_info=job_info,
                original_alert=original_text,
                logs=logs,
            )

            # ── 6. Post rich Block Kit reply in the same thread ──────────────
            client.chat_postMessage(
                channel=channel_id,
                thread_ts=thread_ts,
                blocks=_build_analysis_blocks(analysis, job_info),
                text=(
                    f"Root cause analysis for "
                    f"{job_info.get('dag_id') or job_info.get('job_name', 'pipeline')}: "
                    f"{analysis.get('root_cause', 'See thread for details.')}"
                ),
            )

        except Exception as exc:
            logger.exception("Unhandled error during analysis")
            say(
                text=f"❌ Something went wrong during analysis: `{exc}`",
                thread_ts=thread_ts,
                channel=channel_id,
            )


# ---------------------------------------------------------------------------
# Message parser
# ---------------------------------------------------------------------------

def extract_job_info(text: str) -> dict:
    """
    Parse an Airflow or Glue Slack alert and return a dict with:
      source, dag_id, task_id, run_id, job_name, log_group, log_stream

    Handles a variety of common notification formats including those from
    the Airflow Slack operator, custom webhooks, and AWS EventBridge.
    """
    info: dict = {
        "source": None,
        "dag_id": None,
        "task_id": None,
        "run_id": None,
        "job_name": None,
        "log_group": None,
        "log_stream": None,
    }

    lowered = text.lower()

    # ── Airflow ──────────────────────────────────────────────────────────
    if any(k in lowered for k in ("airflow", "dag", "task failed", "task instance")):
        info["source"] = "airflow"

        # DAG id  — matches "dag_id: my_dag", "DAG: my_dag", "dag `my_dag`"
        dag_match = re.search(
            r"dag[_\s]*(?:id)?[:\s`'\"]+([A-Za-z0-9_\-]+)", text, re.IGNORECASE
        )
        if dag_match:
            info["dag_id"] = dag_match.group(1)

        # Task id
        task_match = re.search(
            r"task[_\s]*id[:\s`'\"]+([A-Za-z0-9_\-]+)"
            r"|\bTask:\s*`?([A-Za-z0-9][A-Za-z0-9_\-]*[_\-][A-Za-z0-9_\-]+)`?",
            text, re.IGNORECASE
        )
        if task_match:
            info["task_id"] = task_match.group(1) or task_match.group(2)

        # Run id — ISO timestamps like scheduled__2024-01-15T06:00:00+00:00
        run_match = re.search(
            r"run[_\s]*(?:id)?[:\s`'\"]+([A-Za-z0-9_\-T:\.+%]+)", text, re.IGNORECASE
        )
        if run_match:
            info["run_id"] = run_match.group(1)

        info["log_group"] = os.environ.get(
            "AIRFLOW_LOG_GROUP", "/aws/mwaa/airflow"
        )

        # Build expected log stream prefix for MWAA
        if info["dag_id"]:
            parts = ["task", info["dag_id"]]
            if info["run_id"]:
                parts.append(info["run_id"])
            if info["task_id"]:
                parts.append(info["task_id"])
            info["log_stream"] = "/".join(parts)

    # ── AWS Glue ─────────────────────────────────────────────────────────
    elif any(k in lowered for k in ("glue", "job run", "etl", "jr_")):
        info["source"] = "glue"

        # Job name — matches "Job: my-job", "job_name: my-job"
        job_match = re.search(
            r"job[_\s]*name[:\s`'\"]+([A-Za-z0-9_\-]+)"
            r"|\bJob:\s*`?([A-Za-z0-9][A-Za-z0-9_\-]*[_\-][A-Za-z0-9_\-]+)`?",
            text, re.IGNORECASE
        )
        if job_match:
            info["job_name"] = job_match.group(1) or job_match.group(2)

        # Glue run id — always starts with "jr_"
        run_match = re.search(r"(jr_[A-Za-z0-9]+)", text)
        if run_match:
            info["run_id"] = run_match.group(1)

        info["log_group"] = os.environ.get(
            "GLUE_ERROR_LOG_GROUP", "/aws-glue/jobs/error"
        )

    return info


# ---------------------------------------------------------------------------
# Slack Block Kit formatter
# ---------------------------------------------------------------------------

_SEVERITY_EMOJI = {
    "critical": "🔴",
    "high": "🟠",
    "medium": "🟡",
    "low": "🟢",
}

_CATEGORY_LABEL = {
    "dependency_failure": "Dependency failure",
    "resource_exhaustion": "Resource exhaustion",
    "permission_error": "Permission / IAM error",
    "data_quality": "Data quality issue",
    "timeout": "Timeout",
    "code_error": "Code error",
    "configuration": "Configuration error",
    "network": "Network error",
    "unknown": "Unknown",
}


def _build_analysis_blocks(analysis: dict, job_info: dict) -> list:
    """Render Claude's structured analysis as Slack Block Kit blocks."""

    job_label = job_info.get("dag_id") or job_info.get("job_name") or "Pipeline"
    source_emoji = "🌊" if job_info.get("source") == "airflow" else "✨"
    severity = analysis.get("severity", "high").lower()
    category = analysis.get("category", "unknown").lower()

    sev_emoji = _SEVERITY_EMOJI.get(severity, "🟠")
    cat_label = _CATEGORY_LABEL.get(category, category.replace("_", " ").title())

    blocks = [
        # ── Header ──────────────────────────────────────────────────────
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{source_emoji} Root Cause Analysis — {job_label}",
                "emoji": True,
            },
        },
        # ── Severity + category chips ────────────────────────────────────
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"{sev_emoji} *Severity:* {severity.capitalize()}   •   🏷️ *Category:* {cat_label}",
                }
            ],
        },
        {"type": "divider"},
        # ── Root cause ───────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🔴 Root Cause*\n{analysis.get('root_cause', '_Not identified_')}",
            },
        },
        # ── Summary ──────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*📋 What Happened*\n{analysis.get('summary', '_N/A_')}",
            },
        },
        # ── Fix ──────────────────────────────────────────────────────────
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*🛠️ Recommended Fix*\n{analysis.get('fix', '_N/A_')}",
            },
        },
    ]

    # ── Affected components (optional) ───────────────────────────────────
    if analysis.get("affected_components"):
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔗 Affected Components*\n{analysis['affected_components']}",
                },
            }
        )

    # ── Key error snippet (optional) ─────────────────────────────────────
    if analysis.get("error_snippet"):
        snippet = analysis["error_snippet"][:2900]  # Slack block text limit
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*📌 Key Error Snippet*\n```{snippet}```",
                },
            }
        )

    blocks.append({"type": "divider"})

    # ── Footer ───────────────────────────────────────────────────────────
    log_group = job_info.get("log_group", "N/A")
    source_upper = (job_info.get("source") or "unknown").upper()
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"🤖 Analyzed by *Claude AI*  •  "
                        f"Source: *{source_upper}*  •  "
                        f"Log group: `{log_group}`"
                    ),
                }
            ],
        }
    )

    return blocks
