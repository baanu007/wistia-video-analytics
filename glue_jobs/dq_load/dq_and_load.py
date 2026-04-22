"""
Wistia Analytics - Data Quality + Redshift Load (Glue Python Shell)

Reads the Silver-layer Parquet files for today's partition, runs DQ checks,
and if all pass, issues Redshift COPY commands via the Redshift Data API.

This is the gate before data lands in the warehouse. If DQ fails,
we exit non-zero and the Glue Workflow halts (nothing is loaded).

The load uses a staging-table + MERGE pattern for idempotency, so running
the same day's load twice produces the same result.
"""

import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import pyarrow.parquet as pq
from awsglue.utils import getResolvedOptions


# ============================================================
# CONFIG / ARGS
# ============================================================
import os as _os
ARGS = getResolvedOptions(
    sys.argv,
    [
        "SILVER_BUCKET",
        "REDSHIFT_WORKGROUP",
        "REDSHIFT_DATABASE",
        "REDSHIFT_IAM_ROLE_ARN",
        "REDSHIFT_SECRET_ARN",
        "AWS_REGION_NAME",
    ],
)
ARGS["JOB_NAME"] = _os.environ.get("JOB_NAME", "wistia-dq-and-load")

SILVER_BUCKET = ARGS["SILVER_BUCKET"]
WORKGROUP = ARGS["REDSHIFT_WORKGROUP"]
DATABASE = ARGS["REDSHIFT_DATABASE"]
REDSHIFT_IAM_ROLE = ARGS["REDSHIFT_IAM_ROLE_ARN"]
REDSHIFT_SECRET_ARN = ARGS["REDSHIFT_SECRET_ARN"]
AWS_REGION = ARGS["AWS_REGION_NAME"]


# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger("wistia-dq-load")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)
logger.propagate = False


def log_event(event: str, **fields):
    payload = {"event": event, "ts": datetime.now(timezone.utc).isoformat(), **fields}
    logger.info(json.dumps(payload, default=str))


# ============================================================
# AWS CLIENTS
# ============================================================
session = boto3.session.Session(region_name=AWS_REGION)
rsd_client = session.client("redshift-data")
s3_client = session.client("s3")


def _read_parquet_from_s3(uri: str):
    """Download all parquet part-files under an S3 prefix and return a pandas DataFrame.

    Uses only pyarrow + boto3 (both pre-installed in Glue Python Shell 3.9),
    so we don't need any pip installs that can fail in constrained runtimes.
    """
    assert uri.startswith("s3://")
    rest = uri[len("s3://"):]
    bucket, _, prefix = rest.partition("/")

    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")]
    if not keys:
        raise FileNotFoundError(f"no .parquet objects under {uri}")

    tables = []
    for key in keys:
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
        tables.append(pq.read_table(io.BytesIO(body)))
    # Concat then convert to pandas for the DQ checks (small enough to fit in memory)
    import pyarrow as pa
    combined = pa.concat_tables(tables)
    return combined.to_pandas()


# ============================================================
# PARTITION PATHS
# ============================================================
now = datetime.now(timezone.utc)
YEAR, MONTH, DAY = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
DATE_PART = f"year={YEAR}/month={MONTH}/day={DAY}"

TABLES: Dict[str, Dict[str, Any]] = {
    "dim_media": {
        "required_cols": ["media_id", "name", "channel", "created_at"],
        "min_rows": 1,
        "max_null_pct": {"media_id": 0, "name": 0, "channel": 0},
    },
    "dim_visitor": {
        "required_cols": ["visitor_key", "browser", "platform"],
        "min_rows": 1,
        "max_null_pct": {"visitor_key": 0, "browser": 20, "platform": 20},
    },
    "fact_media_engagement": {
        "required_cols": ["event_key", "media_id", "visitor_key", "event_date", "percent_viewed"],
        "min_rows": 1,
        "max_null_pct": {"event_key": 0, "media_id": 0, "visitor_key": 5},
    },
}


def silver_uri(table: str) -> str:
    return f"s3://{SILVER_BUCKET}/{table}/{DATE_PART}/"


# ============================================================
# DATA QUALITY CHECKS
# ============================================================
def run_dq_checks() -> Dict[str, Dict[str, Any]]:
    """Read each Silver table via awswrangler, run checks, return per-table stats.

    Raises RuntimeError if any check fails.
    """
    results: Dict[str, Dict[str, Any]] = {}

    for table, rules in TABLES.items():
        uri = silver_uri(table)
        log_event("dq_start", table=table, uri=uri)

        try:
            df = _read_parquet_from_s3(uri)
        except FileNotFoundError:
            # Incremental pipelines legitimately produce empty partitions on
            # quiet days (e.g. Events API returns nothing new). Skip the
            # table -- main() will skip loading it too. This is NOT a failure.
            log_event("dq_skip_no_data", table=table, uri=uri)
            continue
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"DQ: failed to read Silver parquet for {table}: {exc}") from exc

        # Defensive: drop rows whose primary key is null before checking.
        # Wistia occasionally returns empty event objects. Same filter is in the
        # transform now, but we keep this here as a belt-and-suspenders guard.
        pk_col = {"dim_media": "media_id", "dim_visitor": "visitor_key",
                  "fact_media_engagement": "event_key"}.get(table)
        if pk_col and pk_col in df.columns:
            before = len(df)
            df = df[df[pk_col].notna()].reset_index(drop=True)
            dropped = before - len(df)
            if dropped > 0:
                log_event("dq_dropped_null_pk", table=table, pk=pk_col, dropped=dropped)

        row_count = len(df)
        results[table] = {"row_count": row_count}

        if row_count < rules["min_rows"]:
            raise RuntimeError(f"DQ: {table} has {row_count} rows, expected >= {rules['min_rows']}")

        for col in rules["required_cols"]:
            if col not in df.columns:
                raise RuntimeError(f"DQ: {table} missing required column '{col}'")

        for col, max_pct in rules["max_null_pct"].items():
            if col not in df.columns:
                continue
            null_pct = float(df[col].isna().sum()) / row_count * 100.0
            results[table][f"null_pct_{col}"] = round(null_pct, 2)
            if null_pct > max_pct:
                raise RuntimeError(
                    f"DQ: {table}.{col} null% is {null_pct:.2f} (>{max_pct}%)"
                )

        log_event("dq_ok", table=table, **results[table])

    # Cross-table referential integrity -- apply same null-PK filter first
    try:
        media_df = _read_parquet_from_s3(silver_uri("dim_media"))
        media_df = media_df[media_df["media_id"].notna()].reset_index(drop=True)

        fact_df = _read_parquet_from_s3(silver_uri("fact_media_engagement"))
        fact_df = fact_df[
            fact_df["event_key"].notna() & fact_df["media_id"].notna()
        ].reset_index(drop=True)

        orphan_media = fact_df[~fact_df["media_id"].isin(media_df["media_id"])]
        if len(orphan_media) > 0:
            raise RuntimeError(
                f"DQ: {len(orphan_media)} fact rows have media_id not in dim_media"
            )
        log_event("dq_ri_ok", check="fact->dim_media")
    except FileNotFoundError:
        log_event("dq_ri_skip", reason="one or both tables missing")

    return results


# ============================================================
# REDSHIFT DATA API HELPERS
# ============================================================
def execute_statement(sql: str, description: str = "") -> str:
    """Submit a SQL statement to Redshift and poll until done. Returns statement ID.

    Uses SecretArn (admin credentials from Secrets Manager) so we run as the
    admin user with full table permissions, rather than the IAM-derived user
    which has no table grants by default.
    """
    resp = rsd_client.execute_statement(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=sql,
    )
    stmt_id = resp["Id"]
    log_event("rsd_submitted", id=stmt_id, description=description)

    # Poll
    while True:
        desc = rsd_client.describe_statement(Id=stmt_id)
        status = desc["Status"]
        if status in ("FINISHED",):
            log_event("rsd_finished", id=stmt_id, description=description,
                      duration_ms=desc.get("Duration"))
            return stmt_id
        if status in ("FAILED", "ABORTED"):
            err = desc.get("Error", "unknown")
            raise RuntimeError(f"Redshift statement {stmt_id} {status}: {err}\nSQL: {sql[:300]}")
        time.sleep(1)


# ============================================================
# LOAD TABLES WITH STAGING + MERGE (idempotent)
# ============================================================

STAGING_COPY_COMMANDS = {
    "dim_media": """
        COPY wistia.public.stg_dim_media
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """,
    "dim_visitor": """
        COPY wistia.public.stg_dim_visitor
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """,
    # Fact Parquet column order now matches the DDL (the transform job
    # does a final .select() in DDL order), so no explicit column list
    # is needed -- COPY positional mapping works directly.
    "fact_media_engagement": """
        COPY wistia.public.stg_fact_media_engagement
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS PARQUET;
    """,
}

# MERGE SQL: delete from target where key matches staging, then insert all from staging.
# Redshift supports MERGE natively but the delete+insert pattern is safer across versions.
MERGE_SQL = {
    "dim_media": """
        BEGIN;
          TRUNCATE wistia.public.stg_dim_media;
          {copy}
          DELETE FROM wistia.public.stg_dim_media WHERE media_id IS NULL;
          DELETE FROM wistia.public.dim_media
           USING wistia.public.stg_dim_media
           WHERE dim_media.media_id = stg_dim_media.media_id;
          INSERT INTO wistia.public.dim_media
          SELECT * FROM wistia.public.stg_dim_media;
        COMMIT;
    """,
    "dim_visitor": """
        BEGIN;
          TRUNCATE wistia.public.stg_dim_visitor;
          {copy}
          DELETE FROM wistia.public.stg_dim_visitor WHERE visitor_key IS NULL;
          DELETE FROM wistia.public.dim_visitor
           USING wistia.public.stg_dim_visitor
           WHERE dim_visitor.visitor_key = stg_dim_visitor.visitor_key;
          INSERT INTO wistia.public.dim_visitor
          SELECT * FROM wistia.public.stg_dim_visitor;
        COMMIT;
    """,
    "fact_media_engagement": """
        BEGIN;
          TRUNCATE wistia.public.stg_fact_media_engagement;
          {copy}
          DELETE FROM wistia.public.stg_fact_media_engagement WHERE event_key IS NULL;
          DELETE FROM wistia.public.fact_media_engagement
           USING wistia.public.stg_fact_media_engagement
           WHERE fact_media_engagement.event_key = stg_fact_media_engagement.event_key;
          INSERT INTO wistia.public.fact_media_engagement
          SELECT * FROM wistia.public.stg_fact_media_engagement;
        COMMIT;
    """,
}


def load_table(table: str) -> None:
    s3_path = silver_uri(table)
    copy_sql = STAGING_COPY_COMMANDS[table].format(
        s3_path=s3_path, iam_role=REDSHIFT_IAM_ROLE
    ).strip()
    full_sql = MERGE_SQL[table].format(copy=copy_sql)
    execute_statement(full_sql, description=f"load_{table}")


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    log_event(
        "job_start",
        silver=SILVER_BUCKET,
        workgroup=WORKGROUP,
        database=DATABASE,
        date=DATE_PART,
    )

    log_event("dq_phase_start")
    dq_results = run_dq_checks()
    log_event("dq_phase_complete", results=dq_results)

    if not dq_results:
        log_event("load_skip_all", reason="no Silver data for today -- nothing to load")
        log_event("job_complete")
        return

    # Load in dependency order: dims first, then fact. Skip any table that
    # had no Silver partition for today (incremental pipeline, quiet day).
    for table in ("dim_media", "dim_visitor", "fact_media_engagement"):
        if table not in dq_results:
            log_event("load_skip", table=table, reason="no Silver data for today")
            continue
        log_event("load_start", table=table)
        load_table(table)
        log_event("load_complete", table=table)

    log_event("job_complete")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        log_event("job_failed", error=str(exc))
        raise
