"""
tests/test_aws_logs.py — Tests for AWSLogFetcher with mocked boto3.
Run with: pytest tests/test_aws_logs.py -v
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

from src.aws_logs import AWSLogFetcher


def _make_client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "operation")


def _make_event(message: str, offset_seconds: int = 0) -> dict:
    ts = int(datetime(2024, 1, 15, 6, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    return {"timestamp": ts + offset_seconds * 1000, "message": message}


@pytest.fixture()
def fetcher():
    with patch("src.aws_logs.boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        f = AWSLogFetcher()
        f._client = mock_client
        yield f, mock_client


class TestFetchAirflowLogs:

    def test_returns_logs_for_exact_stream(self, fetcher):
        f, client = fetcher
        client.describe_log_streams.return_value = {
            "logStreams": [{"logStreamName": "task/my_dag/run1/t1/attempt=1.log"}]
        }
        client.get_log_events.return_value = {
            "events": [_make_event("Task started"), _make_event("Task failed")],
            "nextForwardToken": "end",
        }

        job_info = {
            "source": "airflow",
            "dag_id": "my_dag",
            "task_id": "t1",
            "run_id": "run1",
            "log_group": "/aws/mwaa/airflow",
        }
        result = f.fetch_logs(job_info)
        assert result is not None
        assert "Task failed" in result

    def test_falls_back_to_dag_prefix(self, fetcher):
        f, client = fetcher
        # First call (exact prefix) returns nothing, second returns a stream
        client.describe_log_streams.side_effect = [
            {"logStreams": []},
            {"logStreams": []},
            {"logStreams": [{"logStreamName": "task/my_dag/run99/t1"}]},
        ]
        client.get_log_events.return_value = {
            "events": [_make_event("Error in task")],
            "nextForwardToken": "end",
        }

        job_info = {
            "source": "airflow",
            "dag_id": "my_dag",
            "task_id": "t1",
            "run_id": "run1",
            "log_group": "/aws/mwaa/airflow",
        }
        result = f.fetch_logs(job_info)
        assert result is not None

    def test_returns_none_when_no_streams(self, fetcher):
        f, client = fetcher
        client.describe_log_streams.return_value = {"logStreams": []}
        job_info = {
            "source": "airflow",
            "dag_id": "my_dag",
            "task_id": "t1",
            "run_id": "run1",
            "log_group": "/aws/mwaa/airflow",
        }
        result = f.fetch_logs(job_info)
        assert result is None

    def test_handles_missing_log_group(self, fetcher):
        f, client = fetcher
        client.describe_log_streams.side_effect = _make_client_error("ResourceNotFoundException")
        job_info = {
            "source": "airflow",
            "dag_id": "my_dag",
            "task_id": "t1",
            "run_id": "run1",
            "log_group": "/aws/mwaa/nonexistent",
        }
        result = f.fetch_logs(job_info)
        assert result is None


class TestFetchGlueLogs:

    def test_returns_error_logs(self, fetcher):
        f, client = fetcher
        client.get_log_events.return_value = {
            "events": [_make_event("PySpark exception: NullPointerException")],
            "nextForwardToken": "end",
        }
        job_info = {
            "source": "glue",
            "job_name": "my-job",
            "run_id": "jr_abc123",
            "log_group": "/aws-glue/jobs/error",
        }
        result = f.fetch_logs(job_info)
        assert result is not None
        assert "NullPointerException" in result

    def test_combines_error_and_output_groups(self, fetcher):
        f, client = fetcher
        client.get_log_events.side_effect = [
            # error group
            {"events": [_make_event("ERROR: Schema mismatch")], "nextForwardToken": "end"},
            # output group
            {"events": [_make_event("INFO: Job started")], "nextForwardToken": "end"},
        ]
        job_info = {
            "source": "glue",
            "job_name": "my-job",
            "run_id": "jr_abc123",
            "log_group": "/aws-glue/jobs/error",
        }
        result = f.fetch_logs(job_info)
        assert "Schema mismatch" in result
        assert "Job started" in result


class TestSmartTruncate:

    def test_short_text_not_truncated(self, fetcher):
        f, _ = fetcher
        text = "short log"
        f.max_log_chars = 1000
        assert f._smart_truncate(text) == text

    def test_long_text_truncated(self, fetcher):
        f, _ = fetcher
        f.max_log_chars = 100
        text = "A" * 200
        result = f._smart_truncate(text)
        assert len(result) < 200
        assert "omitted" in result

    def test_truncated_keeps_head_and_tail(self, fetcher):
        f, _ = fetcher
        f.max_log_chars = 200
        head = "HEAD_CONTENT " * 10
        tail = "TAIL_CONTENT " * 10
        text = head + " MIDDLE " * 100 + tail
        result = f._smart_truncate(text)
        assert "HEAD_CONTENT" in result
        assert "TAIL_CONTENT" in result


class TestRetryLogic:

    def test_retries_on_throttle(self, fetcher):
        f, client = fetcher
        error = _make_client_error("ThrottlingException")
        success = {"logStreams": [{"logStreamName": "stream1"}]}
        client.describe_log_streams.side_effect = [error, success]

        with patch("time.sleep"):
            result = f._find_streams("/group", "prefix")
        assert len(result) == 1
        assert client.describe_log_streams.call_count == 2

    def test_raises_after_max_retries(self, fetcher):
        f, client = fetcher
        client.describe_log_streams.side_effect = _make_client_error("ThrottlingException")
        with patch("time.sleep"):
            result = f._find_streams("/group", "prefix")
        # After exhausting retries the error is swallowed and empty list returned
        assert result == []
