"""
tests/test_claude_analyzer.py — Tests for ClaudeAnalyzer response parsing.
Run with: pytest tests/test_claude_analyzer.py -v
"""

import json
import pytest
from unittest.mock import patch
from src.claude_analyzer import ClaudeAnalyzer


@pytest.fixture()
def analyzer():
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
        return ClaudeAnalyzer()


_JOB_INFO_AIRFLOW = {
    "source": "airflow",
    "dag_id": "sales_pipeline",
    "task_id": "load_s3",
    "run_id": "scheduled__2024-01-01",
    "log_group": "/aws/mwaa/airflow",
}

_VALID_RESPONSE = {
    "root_cause": "S3 bucket 'raw-data' does not exist in us-east-1",
    "summary": "The task failed when attempting to read from a non-existent S3 bucket.",
    "fix": "1. Verify bucket name\n2. Check AWS region\n3. Re-run the task",
    "affected_components": "load_s3, transform_sales",
    "error_snippet": "NoSuchBucket: The specified bucket does not exist",
    "severity": "high",
    "category": "configuration",
}


class TestParseResponse:

    def test_valid_json_parsed(self, analyzer):
        raw = json.dumps(_VALID_RESPONSE)
        result = analyzer._parse_response(raw, _JOB_INFO_AIRFLOW)
        assert result["root_cause"] == _VALID_RESPONSE["root_cause"]
        assert result["severity"] == "high"
        assert result["category"] == "configuration"

    def test_json_with_markdown_fences(self, analyzer):
        raw = f"```json\n{json.dumps(_VALID_RESPONSE)}\n```"
        result = analyzer._parse_response(raw, _JOB_INFO_AIRFLOW)
        assert result["root_cause"] == _VALID_RESPONSE["root_cause"]

    def test_invalid_json_fallback(self, analyzer):
        raw = "Sorry, I could not analyze this log."
        result = analyzer._parse_response(raw, _JOB_INFO_AIRFLOW)
        assert result["root_cause"] == "Analysis complete — see summary below"
        assert result["summary"] == raw
        assert result["severity"] == "high"

    def test_partial_json_fills_defaults(self, analyzer):
        raw = json.dumps({"root_cause": "Timeout after 600s"})
        result = analyzer._parse_response(raw, _JOB_INFO_AIRFLOW)
        assert result["root_cause"] == "Timeout after 600s"
        assert result["fix"] == "Review logs manually."  # default
        assert result["severity"] == "high"  # default

    def test_dag_id_used_in_fallback(self, analyzer):
        raw = "not json"
        result = analyzer._parse_response(raw, _JOB_INFO_AIRFLOW)
        assert result["affected_components"] == "sales_pipeline"


class TestBuildPrompt:

    def test_airflow_prompt_contains_dag(self, analyzer):
        prompt = analyzer._build_prompt(
            "airflow", _JOB_INFO_AIRFLOW, "alert text", "log lines"
        )
        assert "sales_pipeline" in prompt
        assert "load_s3" in prompt
        assert "Apache Airflow" in prompt

    def test_glue_prompt_contains_job_name(self, analyzer):
        glue_info = {
            "source": "glue",
            "job_name": "my-glue-job",
            "run_id": "jr_abc",
            "log_group": "/aws-glue/jobs/error",
        }
        prompt = analyzer._build_prompt("glue", glue_info, "alert", "logs")
        assert "my-glue-job" in prompt
        assert "AWS Glue" in prompt

    def test_original_alert_included(self, analyzer):
        prompt = analyzer._build_prompt(
            "airflow", _JOB_INFO_AIRFLOW, "ORIGINAL ALERT HERE", "logs"
        )
        assert "ORIGINAL ALERT HERE" in prompt

    def test_logs_included(self, analyzer):
        prompt = analyzer._build_prompt(
            "airflow", _JOB_INFO_AIRFLOW, "alert", "ERROR: NullPointerException at line 42"
        )
        assert "NullPointerException" in prompt
