"""
Wistia Analytics - Bronze -> Silver Transformation (Glue PySpark)

Reads raw JSON from the Bronze bucket for today's partition, cleans and
reshapes it into three star-schema tables, and writes Parquet (Snappy) to
the Silver bucket.

Transformations:
    dim_media             = media_metadata JOIN media_stats on media_id,
                            with channel derived from the video name.
    dim_visitor           = visitors rows, deduped on visitor_key;
                            enriched with country/region/city from events
                            (mode per visitor).
    fact_media_engagement = one row per event (event_key), enriched with
                            visitor load_count/play_count.
"""

import re
import sys
from datetime import datetime, timezone
from typing import Optional

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CONFIG / ARGS
# ============================================================
ARGS = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "BRONZE_BUCKET", "SILVER_BUCKET"],
)

BRONZE_BUCKET = ARGS["BRONZE_BUCKET"]
SILVER_BUCKET = ARGS["SILVER_BUCKET"]

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(ARGS["JOB_NAME"], ARGS)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")


def log(event: str, **kwargs):
    print(f"[transform] {event} | " + " ".join(f"{k}={v}" for k, v in kwargs.items()))


# ============================================================
# PATHS
# ============================================================
now = datetime.now(timezone.utc)
YEAR = now.strftime("%Y")
MONTH = now.strftime("%m")
DAY = now.strftime("%d")
DATE_PART = f"year={YEAR}/month={MONTH}/day={DAY}"


def bronze_path(endpoint: str) -> str:
    return f"s3://{BRONZE_BUCKET}/{endpoint}/{DATE_PART}/*"


def silver_path(table: str) -> str:
    return f"s3://{SILVER_BUCKET}/{table}/{DATE_PART}/"


# ============================================================
# LOAD BRONZE
# ============================================================
def read_json(path: str, name: str) -> Optional[DataFrame]:
    """Read JSON from S3; return None if path is empty rather than crash."""
    try:
        df = spark.read.option("multiline", "true").json(path)
        count = df.count()
        log("bronze_read", source=name, rows=count, path=path)
        if count == 0:
            return None
        return df
    except Exception as exc:  # noqa: BLE001
        log("bronze_read_empty", source=name, error=str(exc), path=path)
        return None


def derive_channel(name_col):
    """Derive channel from the media name.

    Video names are e.g.:
        'Chris Face VSL The Gap Method Youtube Paid Ads'
        'Chris Face VSL The Gap Method Facebook Paid Ads'
    """
    lower = F.lower(name_col)
    return (
        F.when(lower.contains("youtube"), F.lit("YouTube"))
        .when(lower.contains("facebook"), F.lit("Facebook"))
        .otherwise(F.lit("Unknown"))
    )


# ============================================================
# BUILD dim_media
# ============================================================
def build_dim_media() -> Optional[DataFrame]:
    meta = read_json(bronze_path("media_metadata"), "media_metadata")
    stats = read_json(bronze_path("media_stats"), "media_stats")

    if meta is None:
        log("dim_media_skip", reason="no media_metadata")
        return None

    meta_clean = meta.select(
        F.col("hashed_id").alias("media_id"),
        F.col("name"),
        F.col("duration").cast("decimal(10,3)").alias("duration_seconds"),
        F.col("created").cast("timestamp").alias("created_at"),
        F.col("updated").cast("timestamp").alias("updated_at"),
        F.col("status"),
        F.col("project.name").alias("project_name"),
        F.col("section").alias("section_name"),
        derive_channel(F.col("name")).alias("channel"),
    )

    if stats is None:
        log("dim_media_no_stats", note="metadata-only")
        final = meta_clean.withColumn("total_plays", F.lit(None).cast("long")) \
                          .withColumn("total_loads", F.lit(None).cast("long")) \
                          .withColumn("play_rate", F.lit(None).cast("decimal(10,6)")) \
                          .withColumn("hours_watched", F.lit(None).cast("decimal(14,6)")) \
                          .withColumn("total_visitors", F.lit(None).cast("long")) \
                          .withColumn("engagement", F.lit(None).cast("decimal(10,6)"))
    else:
        stats_clean = stats.select(
            F.input_file_name().alias("_src_file"),
            F.col("play_count").cast("long").alias("total_plays"),
            F.col("load_count").cast("long").alias("total_loads"),
            F.col("play_rate").cast("decimal(10,6)"),
            F.col("hours_watched").cast("decimal(14,6)"),
            F.col("visitors").cast("long").alias("total_visitors"),
            F.col("engagement").cast("decimal(10,6)"),
        )
        # Stats JSON doesn't include media_id, so extract it from the file path
        # path looks like .../media_stats/year=.../media_id=XXXX/XXXX.json
        stats_with_id = stats_clean.withColumn(
            "media_id",
            F.regexp_extract(F.col("_src_file"), r"media_id=([^/]+)/", 1),
        ).drop("_src_file")

        final = meta_clean.join(stats_with_id, on="media_id", how="left")

    final = final.withColumn("load_date", F.to_date(F.lit(f"{YEAR}-{MONTH}-{DAY}")))
    log("dim_media_built", rows=final.count())
    return final


# ============================================================
# BUILD dim_visitor
# ============================================================
def build_dim_visitor() -> Optional[DataFrame]:
    visitors = read_json(bronze_path("visitors"), "visitors")
    events = read_json(bronze_path("events"), "events")

    if visitors is None:
        log("dim_visitor_skip", reason="no visitors data")
        return None

    # Explode array if the JSON is wrapped in a list (visitors endpoint returns a JSON array)
    # Spark flattens this automatically when multiple rows are present

    visitor_clean = visitors.select(
        F.col("visitor_key"),
        F.col("created_at").cast("timestamp").alias("first_seen_at"),
        F.col("last_active_at").cast("timestamp"),
        F.col("load_count").cast("int"),
        F.col("play_count").cast("int"),
        F.col("user_agent_details.browser").alias("browser"),
        F.col("user_agent_details.browser_version").alias("browser_version"),
        F.col("user_agent_details.platform").alias("platform"),
        F.col("user_agent_details.mobile").cast("boolean").alias("is_mobile"),
    )

    # Dedup visitors by visitor_key (keep latest last_active_at)
    w = Window.partitionBy("visitor_key").orderBy(F.col("last_active_at").desc_nulls_last())
    visitor_clean = visitor_clean.withColumn("_rn", F.row_number().over(w)) \
                                 .filter(F.col("_rn") == 1) \
                                 .drop("_rn")

    # Enrich with geo from events: mode(country/region/city) per visitor
    if events is not None:
        geo = events.select(
            F.col("visitor_key"),
            F.col("country"),
            F.col("region"),
            F.col("city"),
            F.col("ip").alias("last_ip"),
            F.col("received_at").cast("timestamp").alias("_event_ts"),
        )

        # Pick the most recent event's geo per visitor (simpler than mode, avoids ties)
        w_geo = Window.partitionBy("visitor_key").orderBy(F.col("_event_ts").desc_nulls_last())
        geo_latest = geo.withColumn("_rn", F.row_number().over(w_geo)) \
                        .filter(F.col("_rn") == 1) \
                        .drop("_rn", "_event_ts")

        final = visitor_clean.join(geo_latest, on="visitor_key", how="left")
    else:
        final = visitor_clean \
            .withColumn("country", F.lit(None).cast("string")) \
            .withColumn("region", F.lit(None).cast("string")) \
            .withColumn("city", F.lit(None).cast("string")) \
            .withColumn("last_ip", F.lit(None).cast("string"))

    final = final.withColumn("load_date", F.to_date(F.lit(f"{YEAR}-{MONTH}-{DAY}")))
    log("dim_visitor_built", rows=final.count())
    return final


# ============================================================
# BUILD fact_media_engagement
# ============================================================
def build_fact_engagement() -> Optional[DataFrame]:
    events = read_json(bronze_path("events"), "events")
    visitors = read_json(bronze_path("visitors"), "visitors")

    if events is None:
        log("fact_skip", reason="no events data")
        return None

    fact = events.select(
        F.col("event_key"),
        F.col("media_id"),
        F.col("visitor_key"),
        F.to_date(F.col("received_at")).alias("event_date"),
        F.col("received_at").cast("timestamp").alias("received_at"),
        F.col("ip"),
        F.col("country"),
        F.col("region"),
        F.col("city"),
        F.col("lat").cast("decimal(10,6)"),
        F.col("lon").cast("decimal(10,6)"),
        F.col("percent_viewed").cast("decimal(10,6)"),
        F.col("embed_url"),
        F.col("media_name"),
        F.col("user_agent_details.browser").alias("browser"),
        F.col("user_agent_details.platform").alias("platform"),
        F.col("user_agent_details.mobile").cast("boolean").alias("is_mobile"),
    )

    # Drop null-key rows (Wistia occasionally returns an empty event object)
    fact = fact.filter(F.col("event_key").isNotNull() & F.col("media_id").isNotNull())

    # Dedup by event_key (keep latest received_at in case of replays)
    w = Window.partitionBy("event_key").orderBy(F.col("received_at").desc_nulls_last())
    fact = fact.withColumn("_rn", F.row_number().over(w)) \
               .filter(F.col("_rn") == 1) \
               .drop("_rn")

    # Enrich with visitor-level play_count/load_count
    if visitors is not None:
        v_counts = visitors.select(
            F.col("visitor_key"),
            F.col("play_count").cast("int").alias("visitor_play_count"),
            F.col("load_count").cast("int").alias("visitor_load_count"),
        ).dropDuplicates(["visitor_key"])

        fact = fact.join(v_counts, on="visitor_key", how="left")
    else:
        fact = fact.withColumn("visitor_play_count", F.lit(None).cast("int")) \
                   .withColumn("visitor_load_count", F.lit(None).cast("int"))

    fact = fact.withColumn("load_date", F.to_date(F.lit(f"{YEAR}-{MONTH}-{DAY}")))

    # Emit columns in the exact Redshift DDL order so the COPY-by-position
    # works without an explicit column list.
    fact = fact.select(
        "event_key", "media_id", "visitor_key",
        "event_date", "received_at",
        "ip", "country", "region", "city",
        "lat", "lon", "percent_viewed",
        "embed_url", "media_name",
        "browser", "platform", "is_mobile",
        "visitor_play_count", "visitor_load_count",
        "load_date",
    )

    log("fact_built", rows=fact.count())
    return fact


# ============================================================
# DQ (pre-write sanity checks)
# ============================================================
def pre_write_checks(df: DataFrame, table: str, required_cols: list[str]) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{table}: missing required columns {missing}")

    row_count = df.count()
    if row_count == 0:
        raise RuntimeError(f"{table}: produced 0 rows (expected > 0)")

    log("dq_pre_write_ok", table=table, rows=row_count)


def write_parquet(df: DataFrame, table: str) -> None:
    path = silver_path(table)
    df.coalesce(1).write \
      .mode("overwrite") \
      .option("compression", "snappy") \
      .parquet(path)
    log("silver_write", table=table, path=path)


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    log("job_start", bronze=BRONZE_BUCKET, silver=SILVER_BUCKET, date=DATE_PART)

    dim_media = build_dim_media()
    if dim_media is not None:
        pre_write_checks(
            dim_media,
            "dim_media",
            ["media_id", "name", "channel", "created_at"],
        )
        write_parquet(dim_media, "dim_media")

    dim_visitor = build_dim_visitor()
    if dim_visitor is not None:
        pre_write_checks(
            dim_visitor,
            "dim_visitor",
            ["visitor_key", "browser", "platform"],
        )
        write_parquet(dim_visitor, "dim_visitor")

    fact = build_fact_engagement()
    if fact is not None:
        pre_write_checks(
            fact,
            "fact_media_engagement",
            ["event_key", "media_id", "visitor_key", "event_date", "percent_viewed"],
        )
        write_parquet(fact, "fact_media_engagement")

    log("job_complete")
    job.commit()


if __name__ == "__main__":
    main()
