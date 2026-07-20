-- =============================================================================
-- GRIZL Databricks Observability — grizl.observability
-- Source table: grizl.observability.raw_logs (Delta, managed by DLT Auto Loader pipeline)
-- Catalog: grizl  |  Schema: observability
-- =============================================================================
-- How to apply:
--   npm --prefix databricks run sql:observability:dry-run
--   npm --prefix databricks run sql:observability
--
-- Each CREATE statement is separated by a blank line and must be executed
-- individually if your SQL warehouse does not support multi-statement execution.
--
-- Requires: Unity Catalog enabled workspace, grizl catalog and observability schema.
-- Run provision.sh first to create the catalog and schema.
-- =============================================================================


-- ── CATALOG AND SCHEMA ───────────────────────────────────────────────────────

CREATE CATALOG IF NOT EXISTS grizl;

CREATE SCHEMA IF NOT EXISTS grizl.observability
  COMMENT 'GRIZL application observability — raw logs, views, and anomaly signals.';

CREATE SCHEMA IF NOT EXISTS grizl.observability_monitors
  COMMENT 'Lakehouse Monitoring output tables for grizl.observability.raw_logs.';


-- ── RAW LOGS TABLE ───────────────────────────────────────────────────────────
-- This table is normally created and managed by the DLT Auto Loader pipeline
-- (notebooks/01_autoloader_pipeline.py). Run this DDL only when bootstrapping
-- without DLT or when recreating the table schema after a teardown.
--
-- Source: Pub/Sub grizl-log-topic → GCS Cloud Storage export subscription
-- → Auto Loader DLT → this table (Silver quality, partitioned by log_date).

CREATE TABLE IF NOT EXISTS grizl.observability.raw_logs (
  ingest_timestamp   TIMESTAMP  COMMENT 'Timestamp when the event landed in the Delta table',
  source_timestamp   TIMESTAMP  COMMENT 'Original event timestamp from the application',
  service            STRING     COMMENT 'Service name: grizl-backend, grizl-frontend, grizl-log-forwarder',
  environment        STRING     COMMENT 'Deployment environment: production, staging',
  deployment_sha     STRING     COMMENT 'Git commit SHA of the running deployment',
  severity           STRING     COMMENT 'Log severity: DEBUG, INFO, WARNING, ERROR, CRITICAL',
  event_type         STRING     COMMENT 'Structured event type: http_request, deployment, forwarder_start, batch_sent, ...',
  method             STRING     COMMENT 'HTTP method for http_request events',
  route              STRING     COMMENT 'HTTP route path for http_request events',
  status_code        INT        COMMENT 'HTTP status code for http_request events',
  duration_ms        DOUBLE     COMMENT 'Request duration in milliseconds for http_request events',
  trace_id           STRING     COMMENT 'Distributed trace ID',
  request_id         STRING     COMMENT 'HTTP request ID',
  error_type         STRING     COMMENT 'Exception class or error category',
  error_message      STRING     COMMENT 'Error message text',
  error_signature    STRING     COMMENT 'Stable error key: <errorType>:<route>',
  page               STRING     COMMENT 'Browser page path for grizl-frontend events',
  api_status         STRING     COMMENT 'API response status for frontend telemetry events',
  source             STRING     COMMENT 'Event source identifier',
  raw_envelope       STRING     COMMENT 'Raw Pub/Sub envelope for message_skipped events',
  insert_id          STRING     COMMENT 'Cloud Logging insertId for deduplication',
  pubsub_message_id  STRING     COMMENT 'Pub/Sub messageId',
  log_date           DATE       COMMENT 'Partition column: DATE(ingest_timestamp)'
)
USING DELTA
PARTITIONED BY (log_date)
COMMENT 'GRIZL application log events — Pub/Sub → GCS Cloud Storage export → Auto Loader DLT.'
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- After initial data load, run once to optimize query performance:
-- OPTIMIZE grizl.observability.raw_logs ZORDER BY (service, event_type, severity);


-- ── LOGICAL VIEWS ────────────────────────────────────────────────────────────
-- These views are the typed interface over raw_logs and replace the KQL function
-- layer from grizl-house (HttpRequests(), ApplicationErrors(), ...).
-- After creation, query them like tables:
--   SELECT * FROM grizl.observability.http_requests LIMIT 100;

CREATE OR REPLACE VIEW grizl.observability.http_requests
  COMMENT 'HTTP request events from raw_logs. Equivalent to KQL HttpRequests().'
AS
SELECT
  ingest_timestamp,
  source_timestamp,
  service,
  environment,
  deployment_sha,
  severity,
  method,
  route,
  status_code,
  duration_ms,
  trace_id,
  request_id,
  insert_id,
  pubsub_message_id,
  log_date
FROM grizl.observability.raw_logs
WHERE event_type = 'http_request';

CREATE OR REPLACE VIEW grizl.observability.application_errors
  COMMENT 'ERROR and CRITICAL severity events across all services. Equivalent to KQL ApplicationErrors().'
AS
SELECT
  ingest_timestamp,
  source_timestamp,
  service,
  environment,
  deployment_sha,
  severity,
  event_type,
  error_type,
  error_message,
  error_signature,
  route,
  trace_id,
  request_id,
  insert_id,
  pubsub_message_id,
  log_date
FROM grizl.observability.raw_logs
WHERE severity IN ('ERROR', 'CRITICAL');

CREATE OR REPLACE VIEW grizl.observability.frontend_telemetry
  COMMENT 'Browser telemetry from grizl-frontend via POST /api/telemetry. Equivalent to KQL FrontendTelemetry().'
AS
SELECT
  ingest_timestamp,
  source_timestamp,
  service,
  environment,
  severity,
  event_type,
  page,
  api_status,
  error_type,
  error_message,
  source,
  insert_id,
  pubsub_message_id,
  log_date
FROM grizl.observability.raw_logs
WHERE service = 'grizl-frontend';

CREATE OR REPLACE VIEW grizl.observability.deployments
  COMMENT 'Deployment metadata events emitted at container startup. Equivalent to KQL Deployments().'
AS
SELECT
  ingest_timestamp,
  source_timestamp,
  service,
  environment,
  deployment_sha,
  severity,
  insert_id,
  pubsub_message_id,
  log_date
FROM grizl.observability.raw_logs
WHERE event_type = 'deployment';

CREATE OR REPLACE VIEW grizl.observability.forwarder_health
  COMMENT 'grizl-log-forwarder operational events. Equivalent to KQL ForwarderHealth().'
AS
SELECT
  ingest_timestamp,
  source_timestamp,
  service,
  environment,
  severity,
  event_type,
  error_message,
  raw_envelope,
  insert_id,
  pubsub_message_id,
  log_date
FROM grizl.observability.raw_logs
WHERE service = 'grizl-log-forwarder';


-- =============================================================================
-- OPERATIONAL ANALYTICS QUERIES
-- Run these in the Databricks SQL editor for ad-hoc investigation.
-- =============================================================================


-- ── 1. SUMMARY ───────────────────────────────────────────────────────────────
-- Events by service, event_type, and severity — pipeline health overview.

SELECT service, event_type, severity, COUNT(*) AS cnt
FROM grizl.observability.raw_logs
GROUP BY 1, 2, 3
ORDER BY cnt DESC;


-- ── 2. HTTP REQUESTS ─────────────────────────────────────────────────────────

-- All HTTP request events
SELECT ingest_timestamp, service, environment, severity,
       method, route, status_code, duration_ms, trace_id, request_id, deployment_sha
FROM grizl.observability.http_requests
ORDER BY ingest_timestamp DESC
LIMIT 100;

-- HTTP 5xx errors over time (5-minute buckets)
SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
       service, COUNT(*) AS error_count
FROM grizl.observability.http_requests
WHERE status_code >= 500
GROUP BY 1, 2
ORDER BY time_bin DESC;

-- HTTP request volume and error rate over time (5-minute buckets)
SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
       service,
       COUNT(*) AS requests,
       SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)                     AS errors_5xx,
       SUM(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 ELSE 0 END) AS errors_4xx,
       SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END) / COUNT(*)       AS error_rate
FROM grizl.observability.http_requests
GROUP BY 1, 2
ORDER BY time_bin DESC;

-- Slowest routes (p95 latency, last 24 hours)
SELECT route, service,
       PERCENTILE(duration_ms, 0.95) AS p95_ms,
       COUNT(*)                       AS request_count
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND duration_ms IS NOT NULL
GROUP BY 1, 2
ORDER BY p95_ms DESC
LIMIT 25;

-- Top routes by request volume (last 24 hours)
SELECT route, method, service, COUNT(*) AS requests
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2, 3
ORDER BY requests DESC
LIMIT 25;


-- ── 3. APPLICATION ERRORS ────────────────────────────────────────────────────

-- Recent errors and critical events
SELECT ingest_timestamp, service, environment, severity,
       event_type, error_type, error_message, error_signature,
       route, trace_id, request_id, deployment_sha
FROM grizl.observability.application_errors
ORDER BY ingest_timestamp DESC
LIMIT 50;

-- Error rate by service over time (5-minute buckets)
SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
       service,
       SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
       COUNT(*) AS total,
       SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) AS error_rate
FROM grizl.observability.raw_logs
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2
ORDER BY time_bin DESC;

-- Error signature frequency (last 24 hours)
SELECT error_signature, service,
       COUNT(*) AS cnt, MAX(ingest_timestamp) AS last_seen
FROM grizl.observability.application_errors
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND error_signature IS NOT NULL
GROUP BY 1, 2
ORDER BY cnt DESC
LIMIT 25;


-- ── 4. FRONTEND TELEMETRY ────────────────────────────────────────────────────

-- Recent frontend events
SELECT ingest_timestamp, severity, event_type, page, api_status, error_type, error_message, source
FROM grizl.observability.frontend_telemetry
ORDER BY ingest_timestamp DESC
LIMIT 25;

-- Frontend API errors by page (last 24 hours)
SELECT page, error_type, api_status, COUNT(*) AS errors
FROM grizl.observability.frontend_telemetry
WHERE severity IN ('ERROR', 'CRITICAL')
  AND ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2, 3
ORDER BY errors DESC
LIMIT 25;


-- ── 5. DEPLOYMENTS ───────────────────────────────────────────────────────────

-- Recent deployments
SELECT ingest_timestamp, service, environment, deployment_sha, severity
FROM grizl.observability.deployments
ORDER BY ingest_timestamp DESC
LIMIT 25;

-- Errors by deployment SHA
SELECT deployment_sha, service,
       COUNT(*) AS total,
       SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
       ROUND(SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*), 4) AS error_rate
FROM grizl.observability.raw_logs
WHERE deployment_sha IS NOT NULL
GROUP BY 1, 2
ORDER BY errors DESC, total DESC
LIMIT 25;

-- Recent deployments with post-deploy error counts
WITH deploys AS (
  SELECT service, deployment_sha, MIN(ingest_timestamp) AS deployed_at
  FROM grizl.observability.deployments
  GROUP BY 1, 2
),
err_by_deploy AS (
  SELECT service, deployment_sha, COUNT(*) AS post_deploy_errors
  FROM grizl.observability.application_errors
  WHERE deployment_sha IS NOT NULL
  GROUP BY 1, 2
)
SELECT d.deployed_at, d.service, d.deployment_sha,
       COALESCE(e.post_deploy_errors, 0) AS post_deploy_errors
FROM deploys d
LEFT JOIN err_by_deploy e ON d.service = e.service AND d.deployment_sha = e.deployment_sha
ORDER BY d.deployed_at DESC
LIMIT 20;


-- ── 6. FORWARDER HEALTH ──────────────────────────────────────────────────────

-- Recent forwarder events
SELECT ingest_timestamp, severity, event_type, error_message, raw_envelope
FROM grizl.observability.forwarder_health
ORDER BY ingest_timestamp DESC
LIMIT 50;

-- Forwarder skip rate (last 24 hours, 15-minute buckets)
SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 900) * 900) AS time_bin,
       SUM(CASE WHEN event_type = 'batch_sent' THEN 1 ELSE 0 END)           AS batches_sent,
       SUM(CASE WHEN event_type = 'message_skipped' THEN 1 ELSE 0 END)      AS messages_skipped,
       SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END)   AS errors
FROM grizl.observability.forwarder_health
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1
ORDER BY time_bin DESC;


-- ── 7. LAKEHOUSE MONITORING QUERIES ──────────────────────────────────────────
-- Run after notebooks/02_lakehouse_monitor_setup.py has created the monitor
-- and at least one refresh has completed.

-- Current error_rate drift (monitor output)
-- SELECT window_start_time, slice_key, slice_value,
--        error_rate, error_rate_delta, drift_type
-- FROM grizl.observability_monitors.raw_logs_drift_metrics
-- WHERE column_name = 'error_rate'
-- ORDER BY window_start_time DESC
-- LIMIT 50;

-- Data quality profile snapshot
-- SELECT window_start_time, slice_key, slice_value, num_rows, percent_null
-- FROM grizl.observability_monitors.raw_logs_profile
-- ORDER BY window_start_time DESC
-- LIMIT 50;
