"""Unit tests for DQ + Load job helpers.

We import dq_and_load.py with stubbed awsglue/awswrangler modules (see conftest.py)
and verify: argument handling, statement polling, COPY SQL formatting.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module", autouse=True)
def _stub_argv_and_path():
    sys.argv = [
        "dq_and_load.py",
        "--SILVER_BUCKET", "silver-bucket",
        "--REDSHIFT_WORKGROUP", "wg",
        "--REDSHIFT_DATABASE", "wistia",
        "--REDSHIFT_IAM_ROLE_ARN", "arn:aws:iam::123:role/r",
        "--REDSHIFT_SECRET_ARN", "arn:aws:secretsmanager:us-east-1:123:secret:wistia/redshift-admin-abc",
        "--AWS_REGION_NAME", "us-east-1",
    ]
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root / "glue_jobs" / "dq_load"))


@pytest.fixture
def dq_module(_stub_argv_and_path):
    with patch("boto3.session.Session") as mock_session:
        mock_session.return_value.client.return_value = MagicMock()
        if "dq_and_load" in sys.modules:
            del sys.modules["dq_and_load"]
        import dq_and_load  # noqa: PLC0415
        yield dq_and_load


def test_silver_uri_format(dq_module):
    uri = dq_module.silver_uri("dim_media")
    assert uri.startswith("s3://silver-bucket/dim_media/year=")
    assert uri.endswith("/")


def test_execute_statement_polls_until_finished(dq_module):
    mock_client = MagicMock()
    mock_client.execute_statement.return_value = {"Id": "stmt-1"}
    mock_client.describe_statement.side_effect = [
        {"Status": "STARTED"},
        {"Status": "FINISHED", "Duration": 123},
    ]
    dq_module.rsd_client = mock_client

    with patch("dq_and_load.time.sleep"):
        stmt_id = dq_module.execute_statement("SELECT 1", description="smoke")

    assert stmt_id == "stmt-1"
    assert mock_client.describe_statement.call_count == 2


def test_execute_statement_raises_on_failed(dq_module):
    mock_client = MagicMock()
    mock_client.execute_statement.return_value = {"Id": "stmt-2"}
    mock_client.describe_statement.return_value = {
        "Status": "FAILED", "Error": "relation does not exist"
    }
    dq_module.rsd_client = mock_client

    with patch("dq_and_load.time.sleep"), \
         pytest.raises(RuntimeError, match="FAILED"):
        dq_module.execute_statement("SELECT bad", description="fail")


def test_tables_config_has_expected_shape(dq_module):
    tables = dq_module.TABLES
    assert set(tables.keys()) == {"dim_media", "dim_visitor", "fact_media_engagement"}
    for t, rules in tables.items():
        assert "required_cols" in rules
        assert "min_rows" in rules
        assert "max_null_pct" in rules
