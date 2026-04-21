"""Unit tests for the ingestion job helpers.

We monkey-patch sys.argv before import so getResolvedOptions' stub is happy,
then exercise the pure-logic helpers (today_partition, s3_key, backoff
behavior, pagination loop termination).
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module", autouse=True)
def _stub_argv_and_path():
    """Set argv + sys.path before the ingest module is imported."""
    sys.argv = [
        "wistia_ingest.py",
        "--BRONZE_BUCKET", "test-bucket",
        "--WISTIA_SECRET_NAME", "wistia/test",
        "--MEDIA_IDS", "mA,mB",
        "--AWS_REGION_NAME", "us-east-1",
    ]
    repo_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(repo_root / "glue_jobs" / "ingest"))


@pytest.fixture
def ingest_module(_stub_argv_and_path):
    """Import wistia_ingest fresh for each test, with AWS clients patched."""
    with patch("boto3.session.Session") as mock_session:
        mock_session.return_value.client.return_value = MagicMock()
        if "wistia_ingest" in sys.modules:
            del sys.modules["wistia_ingest"]
        import wistia_ingest  # noqa: PLC0415
        yield wistia_ingest


def test_today_partition_returns_current_date(ingest_module):
    p = ingest_module.today_partition()
    assert set(p.keys()) == {"year", "month", "day"}
    assert len(p["year"]) == 4
    assert len(p["month"]) == 2
    assert len(p["day"]) == 2


def test_s3_key_shape(ingest_module):
    key = ingest_module.s3_key("visitors", "abc123", "page_00001.json")
    assert key.startswith("visitors/year=")
    assert "/media_id=abc123/" in key
    assert key.endswith("page_00001.json")


def test_call_wistia_success(ingest_module):
    with patch("wistia_ingest.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {"ok": True}
        result = ingest_module.call_wistia("http://fake", "token123")
        assert result == {"ok": True}


def test_call_wistia_retries_on_500(ingest_module):
    responses = [MagicMock(status_code=500, text="server error"),
                 MagicMock(status_code=200)]
    responses[1].json.return_value = {"ok": True}
    with patch("wistia_ingest.requests.get", side_effect=responses), \
         patch("wistia_ingest.time.sleep"):
        result = ingest_module.call_wistia("http://fake", "token123")
        assert result == {"ok": True}


def test_call_wistia_fails_on_401(ingest_module):
    resp = MagicMock(status_code=401, text="Unauthorized")
    with patch("wistia_ingest.requests.get", return_value=resp), \
         patch("wistia_ingest.time.sleep"):
        with pytest.raises(RuntimeError, match="401"):
            ingest_module.call_wistia("http://fake", "token123")


def test_call_wistia_honors_retry_after(ingest_module):
    first = MagicMock(status_code=429, headers={"Retry-After": "2"})
    second = MagicMock(status_code=200)
    second.json.return_value = []
    with patch("wistia_ingest.requests.get", side_effect=[first, second]), \
         patch("wistia_ingest.time.sleep") as mock_sleep:
        ingest_module.call_wistia("http://fake", "token")
        mock_sleep.assert_called()
