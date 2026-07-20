# Databricks notebook source
# MAGIC %md
# MAGIC # GRIZL Auto Loader Pipeline (Delta Live Tables)
# MAGIC
# MAGIC Streams GRIZL application logs from a GCS Pub/Sub Cloud Storage export
# MAGIC subscription into `grizl.observability.raw_logs` (Delta Silver table).
# MAGIC
# MAGIC ## GCP setup (one-time, no forwarder changes)
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Create the GCS bucket for Pub/Sub log exports
# MAGIC gsutil mb -l US gs://<GRIZL_GCS_LOGS_BUCKET>
# MAGIC
# MAGIC # 2. Create the Cloud Storage export subscription
# MAGIC #    --output-format=text writes each message body as a UTF-8 text line
# MAGIC #    (newline-delimited JSON, one GRIZL log event per line)
# MAGIC gcloud pubsub subscriptions create grizl-logs-gcs-export \
# MAGIC   --topic=grizl-log-topic \
# MAGIC   --cloud-storage-bucket=<GRIZL_GCS_LOGS_BUCKET> \
# MAGIC   --cloud-storage-file-prefix=logs/ \
# MAGIC   --cloud-storage-file-suffix=.jsonl \
# MAGIC   --cloud-storage-max-duration=60s \
# MAGIC   --cloud-storage-output-format=text
# MAGIC
# MAGIC # 3. Grant Databricks service principal read access to the bucket
# MAGIC gsutil iam ch \
# MAGIC   serviceAccount:<DATABRICKS_SA>@<GCP_PROJECT>.iam.gserviceaccount.com:objectViewer \
# MAGIC   gs://<GRIZL_GCS_LOGS_BUCKET>
# MAGIC ```
# MAGIC
# MAGIC ## Databricks deployment
# MAGIC
# MAGIC Deploy as a Delta Live Tables pipeline via the Databricks Asset Bundle:
# MAGIC ```bash
# MAGIC databricks bundle deploy --target dev
# MAGIC ```
# MAGIC Or manually: Workflows → Delta Live Tables → Create pipeline.
# MAGIC Configure:
# MAGIC - Pipeline mode: Triggered (for batch) or Continuous (for near-real-time)
# MAGIC - Target catalog: grizl
# MAGIC - Target schema: observability
# MAGIC - Pipeline parameter `grizl.gcs_logs_path`: `gs://<GRIZL_GCS_LOGS_BUCKET>/logs/`
# MAGIC - Pipeline parameter `grizl.checkpoint_volume`: `/Volumes/grizl/observability/checkpoints/raw_logs_schema`
# MAGIC
# MAGIC ## Table quality
# MAGIC
# MAGIC | Table | Layer | Notes |
# MAGIC |---|---|---|
# MAGIC | `raw_logs_landing` | Bronze | Raw JSON from GCS, schema inferred by Auto Loader |
# MAGIC | `raw_logs` | Silver | Typed, renamed to snake_case, partitioned by log_date |

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType,
)

GCS_LOGS_PATH = spark.conf.get(
    "grizl.gcs_logs_path",
    "gs://<GRIZL_GCS_LOGS_BUCKET>/logs/",
)
CHECKPOINT_VOLUME = spark.conf.get(
    "grizl.checkpoint_volume",
    "/Volumes/grizl/observability/checkpoints/raw_logs_schema",
)


# ── Bronze: raw landing ──────────────────────────────────────────────────────
@dlt.table(
    name="raw_logs_landing",
    comment=(
        "Raw GRIZL log events from the Pub/Sub GCS Cloud Storage export subscription. "
        "Each file is a batch of newline-delimited JSON log events (one per Pub/Sub message). "
        "Schema is inferred and evolved by Auto Loader."
    ),
    table_properties={"quality": "bronze"},
)
def raw_logs_landing():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", CHECKPOINT_VOLUME)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # For GCS directory listing (no GCS notifications needed):
        .option("cloudFiles.useNotifications", "false")
        .load(GCS_LOGS_PATH)
    )


# ── Silver: typed, cleaned raw_logs ─────────────────────────────────────────
@dlt.table(
    name="raw_logs",
    comment=(
        "Typed and cleaned GRIZL application log events. Partitioned by log_date. "
        "Feeds SQL views (http_requests, application_errors, deployments, forwarder_health). "
        "Source: grizl-backend and grizl-frontend on Cloud Run → Cloud Logging → "
        "Pub/Sub grizl-log-topic → GCS Cloud Storage export → Auto Loader."
    ),
    partition_cols=["log_date"],
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
@dlt.expect("valid_service", "service IS NOT NULL")
@dlt.expect("valid_severity", "severity IN ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')")
@dlt.expect_or_drop("valid_ingest_timestamp", "ingest_timestamp IS NOT NULL")
def raw_logs():
    landing = dlt.read_stream("raw_logs_landing")
    return landing.select(
        F.coalesce(
            F.col("ingestTimestamp").cast(TimestampType()),
            F.current_timestamp(),
        ).alias("ingest_timestamp"),
        F.col("sourceTimestamp").cast(TimestampType()).alias("source_timestamp"),
        F.col("service").cast(StringType()),
        F.col("environment").cast(StringType()),
        F.col("deploymentSha").cast(StringType()).alias("deployment_sha"),
        F.col("severity").cast(StringType()),
        F.col("eventType").cast(StringType()).alias("event_type"),
        F.col("method").cast(StringType()),
        F.col("route").cast(StringType()),
        F.col("statusCode").cast(IntegerType()).alias("status_code"),
        F.col("durationMs").cast(DoubleType()).alias("duration_ms"),
        F.col("traceId").cast(StringType()).alias("trace_id"),
        F.col("requestId").cast(StringType()).alias("request_id"),
        F.col("errorType").cast(StringType()).alias("error_type"),
        F.col("errorMessage").cast(StringType()).alias("error_message"),
        F.col("errorSignature").cast(StringType()).alias("error_signature"),
        F.col("page").cast(StringType()),
        F.col("apiStatus").cast(StringType()).alias("api_status"),
        F.col("source").cast(StringType()),
        F.col("rawEnvelope").cast(StringType()).alias("raw_envelope"),
        F.col("insertId").cast(StringType()).alias("insert_id"),
        F.col("pubsubMessageId").cast(StringType()).alias("pubsub_message_id"),
        F.to_date(
            F.coalesce(
                F.col("ingestTimestamp").cast(TimestampType()),
                F.current_timestamp(),
            )
        ).alias("log_date"),
    )
