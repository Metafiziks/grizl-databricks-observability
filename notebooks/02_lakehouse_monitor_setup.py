# Databricks notebook source
# MAGIC %md
# MAGIC # GRIZL Lakehouse Monitoring Setup
# MAGIC
# MAGIC Attaches a Unity Catalog Quality Monitor to `grizl.observability.raw_logs`.
# MAGIC The monitor runs on a schedule and generates two output tables automatically:
# MAGIC
# MAGIC | Output table | Contents |
# MAGIC |---|---|
# MAGIC | `grizl.observability_monitors.raw_logs_profile` | Data quality, completeness, and value distributions per time window and slice |
# MAGIC | `grizl.observability_monitors.raw_logs_drift_metrics` | Drift in custom metrics (error_rate, http_5xx_rate, p95_duration_ms) vs baseline |
# MAGIC
# MAGIC Custom metrics added:
# MAGIC - `error_rate` — fraction of ERROR/CRITICAL severity events per window
# MAGIC - `http_5xx_rate` — fraction of HTTP 5xx responses per window
# MAGIC - `p95_duration_ms` — 95th-percentile request duration per window
# MAGIC - `p99_duration_ms` — 99th-percentile request duration per window
# MAGIC - `deployment_sha_count` — distinct deployment SHAs per window (deploy frequency)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog enabled workspace
# MAGIC - `grizl.observability.raw_logs` Delta table exists with data
# MAGIC - `grizl.observability_monitors` schema created (run sql/grizl-observability.sql first)
# MAGIC - Databricks Runtime 13.3 LTS+ (MLR or standard)
# MAGIC - Run this notebook once; the monitor refreshes automatically on schedule

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorTimeSeries,
    MonitorCustomMetric,
    MonitorCustomMetricType,
)

# COMMAND ----------

CATALOG           = "grizl"
SCHEMA            = "observability"
TABLE             = "raw_logs"
FULL_TABLE        = f"{CATALOG}.{SCHEMA}.{TABLE}"
MONITOR_SCHEMA    = f"{CATALOG}.observability_monitors"
ASSETS_DIR        = "/Shared/grizl/observability/monitors"
BASELINE_TABLE    = None  # Set to a historical snapshot table to enable drift vs baseline

# COMMAND ----------

w = WorkspaceClient()

monitor = w.quality_monitors.create(
    table_name=FULL_TABLE,
    time_series=MonitorTimeSeries(
        timestamp_col="ingest_timestamp",
        granularities=["5 minutes", "1 hour", "1 day"],
    ),
    slicing_exprs=["service", "environment", "severity"],
    custom_metrics=[
        MonitorCustomMetric(
            name="error_rate",
            input_columns=["severity"],
            definition=(
                "AVG(CASE WHEN severity IN ('ERROR', 'CRITICAL') "
                "THEN 1.0 ELSE 0.0 END)"
            ),
            type=MonitorCustomMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        ),
        MonitorCustomMetric(
            name="http_5xx_rate",
            input_columns=["status_code"],
            definition=(
                "AVG(CASE WHEN status_code >= 500 "
                "THEN 1.0 ELSE 0.0 END)"
            ),
            type=MonitorCustomMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        ),
        MonitorCustomMetric(
            name="p95_duration_ms",
            input_columns=["duration_ms"],
            definition="PERCENTILE(duration_ms, 0.95)",
            type=MonitorCustomMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        ),
        MonitorCustomMetric(
            name="p99_duration_ms",
            input_columns=["duration_ms"],
            definition="PERCENTILE(duration_ms, 0.99)",
            type=MonitorCustomMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        ),
        MonitorCustomMetric(
            name="deployment_sha_count",
            input_columns=["deployment_sha"],
            definition="COUNT(DISTINCT deployment_sha)",
            type=MonitorCustomMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        ),
    ],
    baseline_table_name=BASELINE_TABLE,
    assets_dir=ASSETS_DIR,
    output_schema_name=MONITOR_SCHEMA,
)

print(f"Monitor created for {FULL_TABLE}")
print(f"  Status:        {monitor.status}")
print(f"  Profile table: {MONITOR_SCHEMA}.raw_logs_profile")
print(f"  Drift table:   {MONITOR_SCHEMA}.raw_logs_drift_metrics")
print(f"  Dashboard:     {monitor.dashboard_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Trigger an immediate refresh
# MAGIC After creation, the monitor runs on its internal schedule. To force a
# MAGIC first refresh immediately:

w.quality_monitors.run_refresh(table_name=FULL_TABLE)
print("Monitor refresh triggered.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Query the monitor output
# MAGIC Once the first refresh completes (~5–15 minutes), query the output tables:

# MAGIC %sql
# MAGIC -- Error rate per service over time (profile output)
# MAGIC SELECT window_start_time, slice_key, slice_value, error_rate, num_rows
# MAGIC FROM grizl.observability_monitors.raw_logs_profile
# MAGIC WHERE metric_name = 'error_rate'
# MAGIC   AND slice_key = 'service'
# MAGIC ORDER BY window_start_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drift detection output — any column with detected drift
# MAGIC SELECT window_start_time, column_name, slice_key, slice_value,
# MAGIC        metric_value, baseline_metric_value, delta_pct, drift_type
# MAGIC FROM grizl.observability_monitors.raw_logs_drift_metrics
# MAGIC WHERE drift_type IS NOT NULL
# MAGIC ORDER BY window_start_time DESC
# MAGIC LIMIT 50;

# COMMAND ----------
# MAGIC %md
# MAGIC ## SQL Alert on drift metrics
# MAGIC Create a Databricks SQL Alert pointing at this query to get notified
# MAGIC when Lakehouse Monitoring detects drift in any custom metric:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT window_start_time, column_name, slice_value, metric_value, delta_pct
# MAGIC FROM grizl.observability_monitors.raw_logs_drift_metrics
# MAGIC WHERE drift_type IS NOT NULL
# MAGIC   AND window_start_time >= current_timestamp() - INTERVAL 2 HOURS;
# MAGIC ```
# MAGIC Alert condition: Query returns at least 1 row.
# MAGIC Notification: POST to `https://<backend>/api/databricks/incidents`.
