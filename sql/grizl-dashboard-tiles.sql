-- =============================================================================
-- GRIZL Databricks SQL Dashboard Tile Queries — grizl.observability
-- =============================================================================
-- How to use:
--   1. Open Databricks SQL → Dashboards → Create dashboard.
--   2. For each tile, click "Add visualization" → paste the query.
--   3. Set visualization type and title as indicated.
--   4. Attach the dashboard to a SQL warehouse (same warehouse used for views).
--
-- Requires views from sql/grizl-observability.sql:
--   grizl.observability.http_requests
--   grizl.observability.application_errors
--   grizl.observability.frontend_telemetry
--   grizl.observability.deployments
--   grizl.observability.forwarder_health
-- =============================================================================


-- ── TILE 1: Pipeline overview — events by service / event_type / severity ─────
-- Title:       Events by service / event type / severity
-- Visual type: Table
-- Purpose:     Confirms all services and event types are landing in raw_logs.

SELECT service, event_type, severity, COUNT(*) AS cnt
FROM grizl.observability.raw_logs
GROUP BY 1, 2, 3
ORDER BY cnt DESC;


-- ── TILE 2: HTTP request volume over time ─────────────────────────────────────
-- Title:       HTTP request volume over time
-- Visual type: Line chart — X: time_bin, Y: requests, Group by: service
-- Purpose:     Backend traffic trend. A sudden drop or spike signals an anomaly.

SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
       service, COUNT(*) AS requests
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 6 HOURS
GROUP BY 1, 2
ORDER BY time_bin ASC;


-- ── TILE 3: HTTP error rate over time ─────────────────────────────────────────
-- Title:       HTTP error rate over time (4xx / 5xx)
-- Visual type: Line chart — X: time_bin, Y: errors_4xx / errors_5xx, Group by: service
-- Purpose:     Error rates over time. Useful for spotting regressions after a deploy.

SELECT TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
       service,
       COUNT(*)                                                                    AS requests,
       SUM(CASE WHEN status_code >= 400 AND status_code < 500 THEN 1 ELSE 0 END) AS errors_4xx,
       SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)                       AS errors_5xx
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 6 HOURS
GROUP BY 1, 2
ORDER BY time_bin ASC;


-- ── TILE 4: Top backend routes by error count ─────────────────────────────────
-- Title:       Top backend routes by error count (last 24 hours)
-- Visual type: Bar chart — X: route, Y: error_count

SELECT route, status_code, service, COUNT(*) AS error_count
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND (status_code >= 400 OR severity IN ('ERROR', 'CRITICAL'))
GROUP BY 1, 2, 3
ORDER BY error_count DESC
LIMIT 20;


-- ── TILE 5: Recent application errors ─────────────────────────────────────────
-- Title:       Recent application errors
-- Visual type: Table
-- Purpose:     Live feed of the most recent ERROR/CRITICAL events for triage.

SELECT ingest_timestamp, service, environment, severity,
       event_type, error_type, error_message, error_signature,
       route, deployment_sha, trace_id, request_id
FROM grizl.observability.application_errors
ORDER BY ingest_timestamp DESC
LIMIT 50;


-- ── TILE 6: Application errors by signature ───────────────────────────────────
-- Title:       Application errors by signature (last 24 hours)
-- Visual type: Bar chart — X: error_signature, Y: cnt

SELECT error_signature, error_type, service,
       COUNT(*) AS cnt, MAX(ingest_timestamp) AS last_seen
FROM grizl.observability.application_errors
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND error_signature IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY cnt DESC
LIMIT 25;


-- ── TILE 7: Frontend telemetry errors by page ─────────────────────────────────
-- Title:       Frontend errors by page (last 24 hours)
-- Visual type: Bar chart — X: page, Y: errors

SELECT page, error_type, event_type, COUNT(*) AS errors
FROM grizl.observability.frontend_telemetry
WHERE severity IN ('ERROR', 'CRITICAL')
  AND ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY 1, 2, 3
ORDER BY errors DESC
LIMIT 25;


-- ── TILE 8: Recent frontend telemetry feed ────────────────────────────────────
-- Title:       Recent frontend telemetry
-- Visual type: Table

SELECT ingest_timestamp, severity, event_type, page,
       api_status, error_type, error_message, source
FROM grizl.observability.frontend_telemetry
ORDER BY ingest_timestamp DESC
LIMIT 50;


-- ── TILE 9: Deployment timeline ───────────────────────────────────────────────
-- Title:       Deployment timeline (last 30 days)
-- Visual type: Table

SELECT ingest_timestamp, service, environment, deployment_sha, severity
FROM grizl.observability.deployments
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY ingest_timestamp DESC;


-- ── TILE 10: Errors by deployment SHA ────────────────────────────────────────
-- Title:       Errors by deployment SHA
-- Visual type: Table

WITH deploys AS (
  SELECT service, deployment_sha, MIN(ingest_timestamp) AS deployed_at
  FROM grizl.observability.deployments
  GROUP BY 1, 2
),
backend_errors AS (
  SELECT service, deployment_sha,
    COUNT(*) AS total_errors,
    COUNT(DISTINCT error_type) AS distinct_error_types
  FROM grizl.observability.application_errors
  WHERE deployment_sha IS NOT NULL
  GROUP BY 1, 2
),
frontend_errors AS (
  SELECT service, deployment_sha, COUNT(*) AS frontend_errors
  FROM grizl.observability.frontend_telemetry
  WHERE severity IN ('ERROR', 'CRITICAL') AND deployment_sha IS NOT NULL
  GROUP BY 1, 2
)
SELECT d.deployed_at, d.service, d.deployment_sha,
       COALESCE(be.total_errors, 0)        AS total_errors,
       COALESCE(fe.frontend_errors, 0)     AS frontend_errors,
       COALESCE(be.distinct_error_types, 0) AS distinct_error_types
FROM deploys d
LEFT JOIN backend_errors  be ON d.service = be.service AND d.deployment_sha = be.deployment_sha
LEFT JOIN frontend_errors fe ON d.service = fe.service AND d.deployment_sha = fe.deployment_sha
ORDER BY d.deployed_at DESC
LIMIT 20;


-- ── TILE 11: Forwarder alive check ────────────────────────────────────────────
-- Title:       Forwarder alive check
-- Visual type: Table
-- Purpose:     Shows the most recent forwarder health events. A stale timestamp
--              means the forwarder may be down or disconnected from Pub/Sub.

SELECT ingest_timestamp, severity, event_type, error_message
FROM grizl.observability.forwarder_health
WHERE event_type IN ('forwarder_start', 'batch_sent', 'batch_acked',
                     'health_check', 'ready_check')
ORDER BY ingest_timestamp DESC
LIMIT 25;


-- ── TILE 12: Forwarder skipped / failure events ───────────────────────────────
-- Title:       Forwarder skipped / failure events (last 24 hours)
-- Visual type: Table

SELECT ingest_timestamp, severity, event_type, error_message, raw_envelope
FROM grizl.observability.forwarder_health
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 24 HOURS
  AND (
    event_type IN ('message_skipped', 'batch_retry', 'batch_nack', 'batch_failed')
    OR severity IN ('ERROR', 'CRITICAL')
  )
ORDER BY ingest_timestamp DESC
LIMIT 50;


-- ── TILE 13: Active anomaly signals ───────────────────────────────────────────
-- Title:       Active anomaly signals (last 15 minutes)
-- Visual type: Table
-- Purpose:     Live feed of triggered anomalies across all signal views.
--              This is the primary ML observability tile for operator triage.

SELECT ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
       service, route, error_type, error_signature, deployment_sha,
       anomaly_score, baseline, actual, expected, sql_view
FROM grizl.observability.grizl_recent_anomaly_signals
ORDER BY anomaly_score DESC
LIMIT 50;


-- ── TILE 14: Lakehouse Monitoring — error_rate drift ─────────────────────────
-- Title:       Error rate drift over time (Lakehouse Monitoring)
-- Visual type: Line chart — X: window_start_time, Y: error_rate, Group by: slice_value
-- Purpose:     Managed drift detection from Lakehouse Monitor profiles.
-- Requires:    notebooks/02_lakehouse_monitor_setup.py has run at least once.

-- SELECT window_start_time, slice_key, slice_value,
--        error_rate, error_rate_delta, drift_type
-- FROM grizl.observability_monitors.raw_logs_drift_metrics
-- WHERE column_name = 'error_rate'
--   AND window_start_time >= current_timestamp() - INTERVAL 24 HOURS
-- ORDER BY window_start_time ASC;


-- =============================================================================
-- VALIDATION QUERIES
-- Run these to confirm data is flowing before building the dashboard.
-- =============================================================================

-- Pipeline data present
SELECT service, event_type, severity, COUNT(*) AS cnt
FROM grizl.observability.raw_logs
GROUP BY 1, 2, 3
ORDER BY cnt DESC;

-- Spot-check each view
SELECT * FROM grizl.observability.http_requests LIMIT 10;
SELECT * FROM grizl.observability.application_errors LIMIT 10;
SELECT * FROM grizl.observability.frontend_telemetry LIMIT 10;
SELECT * FROM grizl.observability.deployments LIMIT 10;
SELECT * FROM grizl.observability.forwarder_health LIMIT 10;
