"""
tests/test_bot.py — Unit tests for message parsing and Block Kit formatting.
Run with: pytest tests/test_bot.py -v
"""

from src.bot import extract_job_info, _build_analysis_blocks


class TestExtractJobInfo:
    """Tests for extract_job_info()"""

    # ── Airflow cases ────────────────────────────────────────────────────

    def test_airflow_standard_alert(self):
        text = (
            "🚨 Airflow Task Failed\n"
            "DAG: my_etl_pipeline\n"
            "Task: load_customers\n"
            "Run ID: scheduled__2024-01-15T06:00:00+00:00"
        )
        info = extract_job_info(text)
        assert info["source"] == "airflow"
        assert info["dag_id"] == "my_etl_pipeline"
        assert info["task_id"] == "load_customers"
        assert "2024-01-15" in info["run_id"]

    def test_airflow_lowercase_dag(self):
        text = "dag_id: sales_report task_id: generate_csv"
        info = extract_job_info(text)
        assert info["source"] == "airflow"
        assert info["dag_id"] == "sales_report"

    def test_airflow_no_run_id(self):
        text = "Airflow failure: dag `ingest_events` task `validate`"
        info = extract_job_info(text)
        assert info["source"] == "airflow"
        assert info["dag_id"] == "ingest_events"
        assert info["run_id"] is None

    def test_airflow_log_stream_built(self):
        text = "DAG: my_dag Run ID: manual__2024-01-01 Task: t1"
        info = extract_job_info(text)
        assert info["log_stream"].startswith("task/my_dag")

    # ── Glue cases ───────────────────────────────────────────────────────

    def test_glue_standard_alert(self):
        text = (
            "🚨 AWS Glue Job Failed\n"
            "Job: glue-customer-transform\n"
            "Run ID: jr_abc123def456\n"
            "Status: FAILED"
        )
        info = extract_job_info(text)
        assert info["source"] == "glue"
        assert info["job_name"] == "glue-customer-transform"
        assert info["run_id"] == "jr_abc123def456"

    def test_glue_run_id_only(self):
        text = "AWS Glue job failed jr_xyz789"
        info = extract_job_info(text)
        assert info["source"] == "glue"
        assert info["run_id"] == "jr_xyz789"

    def test_glue_etl_keyword(self):
        text = "ETL job failure detected in production"
        info = extract_job_info(text)
        assert info["source"] == "glue"

    # ── Unknown ──────────────────────────────────────────────────────────

    def test_unknown_source(self):
        text = "Server maintenance scheduled for tomorrow"
        info = extract_job_info(text)
        assert info["source"] is None

    def test_empty_string(self):
        info = extract_job_info("")
        assert info["source"] is None


class TestBuildAnalysisBlocks:
    """Tests for _build_analysis_blocks()"""

    _SAMPLE_ANALYSIS = {
        "root_cause": "S3 key not found: s3://bucket/path/missing.parquet",
        "summary": "The task failed because the upstream file was not created.",
        "fix": "1. Check upstream DAG\n2. Re-run the pipeline",
        "affected_components": "load_customers, transform_orders",
        "error_snippet": "FileNotFoundError: s3://bucket/path/missing.parquet",
        "severity": "high",
        "category": "dependency_failure",
    }

    _AIRFLOW_JOB_INFO = {
        "source": "airflow",
        "dag_id": "my_dag",
        "task_id": "load_data",
        "log_group": "/aws/mwaa/airflow",
    }

    def test_returns_list(self):
        blocks = _build_analysis_blocks(self._SAMPLE_ANALYSIS, self._AIRFLOW_JOB_INFO)
        assert isinstance(blocks, list)
        assert len(blocks) > 0

    def test_header_contains_dag_id(self):
        blocks = _build_analysis_blocks(self._SAMPLE_ANALYSIS, self._AIRFLOW_JOB_INFO)
        header = next(b for b in blocks if b["type"] == "header")
        assert "my_dag" in header["text"]["text"]

    def test_root_cause_present(self):
        blocks = _build_analysis_blocks(self._SAMPLE_ANALYSIS, self._AIRFLOW_JOB_INFO)
        texts = [
            b.get("text", {}).get("text", "")
            for b in blocks
            if b["type"] == "section"
        ]
        combined = "\n".join(texts)
        assert "S3 key not found" in combined

    def test_glue_source_label(self):
        glue_info = {**self._AIRFLOW_JOB_INFO, "source": "glue", "job_name": "my-job"}
        blocks = _build_analysis_blocks(self._SAMPLE_ANALYSIS, glue_info)
        footer = blocks[-1]
        elements_text = footer["elements"][0]["text"]
        assert "GLUE" in elements_text

    def test_missing_snippet_skipped(self):
        analysis = {**self._SAMPLE_ANALYSIS, "error_snippet": ""}
        blocks = _build_analysis_blocks(analysis, self._AIRFLOW_JOB_INFO)
        texts = [
            b.get("text", {}).get("text", "")
            for b in blocks
            if b["type"] == "section"
        ]
        assert not any("Key Error Snippet" in t for t in texts)

    def test_severity_emoji_high(self):
        blocks = _build_analysis_blocks(self._SAMPLE_ANALYSIS, self._AIRFLOW_JOB_INFO)
        context_blocks = [b for b in blocks if b["type"] == "context"]
        text = context_blocks[0]["elements"][0]["text"]
        assert "🟠" in text  # high severity

    def test_severity_emoji_critical(self):
        analysis = {**self._SAMPLE_ANALYSIS, "severity": "critical"}
        blocks = _build_analysis_blocks(analysis, self._AIRFLOW_JOB_INFO)
        context_blocks = [b for b in blocks if b["type"] == "context"]
        text = context_blocks[0]["elements"][0]["text"]
        assert "🔴" in text
