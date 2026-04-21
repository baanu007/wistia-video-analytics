"""
Wistia Analytics - Ingestion Job (Glue Python Shell)

Pulls data from 4 Wistia API endpoints for each configured media ID and
writes the raw JSON responses to the Bronze S3 bucket, partitioned by date.

Endpoints:
    1. Media Metadata         GET /v1/medias/{id}.json
    2. Media Stats            GET /v1/stats/medias/{id}.json
    3. Visitors (paginated)   GET /v1/stats/visitors.json?media_id={id}
    4. Events   (paginated)   GET /v1/stats/events.json?media_id={id}

Features:
    - Retry with exponential backoff on 429 and 5xx responses
    - Pagination loop (page=1,2,3...) until empty response
    - Watermark-based incremental pulls (SSM Parameter Store)
    - Structured JSON logging to CloudWatch
    - Hard-fails with non-zero exit on unrecoverable errors, which
      causes the Glue Workflow to halt before Transform runs.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import requests
from awsglue.utils import getResolvedOptions


# ============================================================
# CONFIG / ARGS
# ============================================================
ARGS = getResolvedOptions(
    sys.argv,
    [
        "BRONZE_BUCKET",
        "WISTIA_SECRET_NAME",
        "MEDIA_IDS",
        "AWS_REGION_NAME",
    ],
)
ARGS["JOB_NAME"] = os.environ.get("JOB_NAME", "wistia-ingest")

BRONZE_BUCKET = ARGS["BRONZE_BUCKET"]
SECRET_NAME = ARGS["WISTIA_SECRET_NAME"]
MEDIA_IDS = [m.strip() for m in ARGS["MEDIA_IDS"].split(",") if m.strip()]
AWS_REGION = ARGS["AWS_REGION_NAME"]

WISTIA_BASE_URL = "https://api.wistia.com/v1"
PAGE_SIZE = 100
MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1.0

# Absolute ceiling only to protect against a malfunctioning "empty page"
# signal on the Wistia side. This is NOT a functional limit; normal runs
# should stop long before this via the empty-response check OR the
# watermark-based filter that skips already-ingested records.
MAX_PAGES_SAFETY = 50000


# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger("wistia-ingest")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.propagate = False


def log_event(event: str, **fields: Any) -> None:
    """Emit a structured JSON log line."""
    payload = {"event": event, "ts": datetime.now(timezone.utc).isoformat(), **fields}
    logger.info(json.dumps(payload, default=str))


# ============================================================
# AWS CLIENTS
# ============================================================
session = boto3.session.Session(region_name=AWS_REGION)
secrets_client = session.client("secretsmanager")
s3_client = session.client("s3")
ssm_client = session.client("ssm")


def get_wistia_token() -> str:
    """Fetch the Wistia Bearer token from Secrets Manager."""
    resp = secrets_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(resp["SecretString"])
    return secret["api_token"]


# ============================================================
# WATERMARKS
# ============================================================
def get_watermark(endpoint: str, media_id: str) -> Optional[str]:
    """Read the last-seen timestamp watermark for an endpoint + media."""
    param = f"/wistia/watermark/{endpoint}/{media_id}"
    try:
        resp = ssm_client.get_parameter(Name=param)
        value = resp["Parameter"]["Value"]
        if value and value != "1970-01-01T00:00:00Z":
            return value
        return None
    except ssm_client.exceptions.ParameterNotFound:
        log_event("watermark_not_found", parameter=param)
        return None


def set_watermark(endpoint: str, media_id: str, value: str) -> None:
    param = f"/wistia/watermark/{endpoint}/{media_id}"
    ssm_client.put_parameter(
        Name=param,
        Value=value,
        Type="String",
        Overwrite=True,
    )
    log_event("watermark_updated", parameter=param, value=value)


# ============================================================
# HTTP WITH RETRY + BACKOFF
# ============================================================
def call_wistia(
    url: str,
    token: str,
    params: Optional[Dict[str, Any]] = None,
) -> Any:
    """GET a Wistia endpoint with retry, exponential backoff, and rate-limit handling.

    Retries on:
        - requests exceptions (network/DNS/timeout)
        - HTTP 429 (rate limit) -- respects Retry-After if present
        - HTTP 5xx
    Fails hard on 4xx (other than 429) -- indicates a bug or auth failure.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "wistia-analytics-pipeline/1.0",
    }

    backoff = INITIAL_BACKOFF_SECONDS
    last_error: Optional[str] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
        except requests.RequestException as exc:
            last_error = f"network error: {exc}"
            log_event("api_retry", url=url, attempt=attempt, reason=last_error, backoff=backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", int(backoff)))
            last_error = f"rate limited (429)"
            log_event(
                "api_rate_limited",
                url=url,
                attempt=attempt,
                retry_after_seconds=retry_after,
            )
            time.sleep(retry_after)
            backoff *= 2
            continue

        if 500 <= response.status_code < 600:
            last_error = f"server error ({response.status_code})"
            log_event(
                "api_retry",
                url=url,
                attempt=attempt,
                status=response.status_code,
                backoff=backoff,
            )
            time.sleep(backoff)
            backoff *= 2
            continue

        # Any other 4xx is fatal
        raise RuntimeError(
            f"Wistia API error {response.status_code} on {url}: {response.text[:400]}"
        )

    raise RuntimeError(
        f"Wistia API exhausted {MAX_RETRIES} retries for {url}: {last_error}"
    )


# ============================================================
# S3 WRITE HELPERS
# ============================================================
def today_partition() -> Dict[str, str]:
    now = datetime.now(timezone.utc)
    return {
        "year": now.strftime("%Y"),
        "month": now.strftime("%m"),
        "day": now.strftime("%d"),
    }


def s3_key(endpoint: str, media_id: str, file_name: str) -> str:
    p = today_partition()
    return (
        f"{endpoint}/year={p['year']}/month={p['month']}/day={p['day']}/"
        f"media_id={media_id}/{file_name}"
    )


def write_json_to_s3(key: str, payload: Any) -> None:
    body = json.dumps(payload, default=str).encode("utf-8")
    s3_client.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    log_event("s3_write", bucket=BRONZE_BUCKET, key=key, bytes=len(body))


# ============================================================
# ENDPOINT HANDLERS
# ============================================================
def ingest_media_metadata(media_id: str, token: str) -> None:
    """Endpoint 1: /v1/medias/{id}.json -- single object response."""
    url = f"{WISTIA_BASE_URL}/medias/{media_id}.json"
    data = call_wistia(url, token)
    key = s3_key("media_metadata", media_id, f"{media_id}.json")
    write_json_to_s3(key, data)


def ingest_media_stats(media_id: str, token: str) -> None:
    """Endpoint 2: /v1/stats/medias/{id}.json -- single object response."""
    url = f"{WISTIA_BASE_URL}/stats/medias/{media_id}.json"
    data = call_wistia(url, token)
    key = s3_key("media_stats", media_id, f"{media_id}.json")
    write_json_to_s3(key, data)


def ingest_visitors(media_id: str, token: str) -> None:
    """Endpoint 3: /v1/stats/visitors.json?media_id={id} -- paginated list.

    Implements FR6 (pagination to fetch ALL pages) and FR7 (incremental
    based on created_at) together:
      * We page through in created_at DESCENDING order (newest first).
      * We read the last-seen watermark for this media.
      * We keep the page if ANY row is newer than the watermark; otherwise
        we've caught up and can stop paginating.
      * After the run, the highest created_at becomes the new watermark,
        so next run only pulls NEW records.
    """
    url = f"{WISTIA_BASE_URL}/stats/visitors.json"
    watermark = get_watermark("visitors", media_id)
    log_event("visitors_start", media_id=media_id, watermark=watermark)
    latest_ts: Optional[str] = None

    for page in range(1, MAX_PAGES_SAFETY + 1):
        params = {"media_id": media_id, "page": page, "per_page": PAGE_SIZE}
        rows: List[Dict[str, Any]] = call_wistia(url, token, params=params)

        if not rows:
            log_event("visitors_pagination_done", media_id=media_id, last_page=page - 1)
            break

        # Track newest created_at for the next watermark
        for row in rows:
            ts = row.get("created_at")
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts

        # Incremental short-circuit: if this whole page is at-or-below watermark,
        # we've seen everything from here down on a previous run -- stop.
        if watermark:
            new_rows = [r for r in rows if (r.get("created_at") or "") > watermark]
            if not new_rows:
                log_event(
                    "visitors_caught_up",
                    media_id=media_id,
                    page=page,
                    watermark=watermark,
                )
                break
            rows = new_rows

        key = s3_key("visitors", media_id, f"page_{page:05d}.json")
        write_json_to_s3(key, rows)

        if len(rows) < PAGE_SIZE:
            log_event("visitors_last_page_short", media_id=media_id, page=page, rows=len(rows))
            break
    else:
        raise RuntimeError(f"visitors: exceeded MAX_PAGES_SAFETY={MAX_PAGES_SAFETY}")

    if latest_ts:
        set_watermark("visitors", media_id, latest_ts)


def ingest_events(media_id: str, token: str) -> None:
    """Endpoint 4: /v1/stats/events.json?media_id={id} -- paginated list.

    Same incremental approach as visitors, but keyed on received_at.
    FR6 + FR7 together: day 1 backfills everything, day 2+ short-circuits
    once we reach rows below the last watermark.
    """
    url = f"{WISTIA_BASE_URL}/stats/events.json"
    watermark = get_watermark("events", media_id)
    log_event("events_start", media_id=media_id, watermark=watermark)
    latest_ts: Optional[str] = None

    for page in range(1, MAX_PAGES_SAFETY + 1):
        params = {"media_id": media_id, "page": page, "per_page": PAGE_SIZE}
        rows: List[Dict[str, Any]] = call_wistia(url, token, params=params)

        if not rows:
            log_event("events_pagination_done", media_id=media_id, last_page=page - 1)
            break

        for row in rows:
            ts = row.get("received_at")
            if ts and (latest_ts is None or ts > latest_ts):
                latest_ts = ts

        if watermark:
            new_rows = [r for r in rows if (r.get("received_at") or "") > watermark]
            if not new_rows:
                log_event(
                    "events_caught_up",
                    media_id=media_id,
                    page=page,
                    watermark=watermark,
                )
                break
            rows = new_rows

        key = s3_key("events", media_id, f"page_{page:05d}.json")
        write_json_to_s3(key, rows)

        if len(rows) < PAGE_SIZE:
            log_event("events_last_page_short", media_id=media_id, page=page, rows=len(rows))
            break
    else:
        raise RuntimeError(f"events: exceeded MAX_PAGES_SAFETY={MAX_PAGES_SAFETY}")

    if latest_ts:
        set_watermark("events", media_id, latest_ts)


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    log_event(
        "job_start",
        job_name=ARGS.get("JOB_NAME"),
        media_ids=MEDIA_IDS,
        bronze_bucket=BRONZE_BUCKET,
        region=AWS_REGION,
    )

    token = get_wistia_token()
    log_event("token_fetched")

    summary: Dict[str, Dict[str, str]] = {}

    for media_id in MEDIA_IDS:
        per_media: Dict[str, str] = {}

        for endpoint_name, handler in (
            ("media_metadata", ingest_media_metadata),
            ("media_stats", ingest_media_stats),
            ("visitors", ingest_visitors),
            ("events", ingest_events),
        ):
            try:
                handler(media_id, token)
                per_media[endpoint_name] = "success"
            except Exception as exc:  # noqa: BLE001
                per_media[endpoint_name] = f"failed: {exc}"
                log_event(
                    "endpoint_failed",
                    media_id=media_id,
                    endpoint=endpoint_name,
                    error=str(exc),
                )
                # Raise -> Glue job fails -> workflow halts
                raise

        summary[media_id] = per_media

    log_event("job_complete", summary=summary)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log_event("job_failed", error=str(exc))
        raise
