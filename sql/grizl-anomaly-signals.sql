-- =============================================================================
-- GRIZL Databricks ML-observability anomaly signal views — grizl.observability
-- =============================================================================
-- How to apply:
--   npm --prefix databricks run sql:anomaly-signals:dry-run
--   npm --prefix databricks run sql:anomaly-signals
--
-- These views use z-score anomaly detection over a rolling baseline window,
-- equivalent to the KQL series_decompose_anomalies() layer in grizl-house.
--
-- The baseline window covers the last 2 days, excluding the most recent 15 minutes.
-- The detection window is the last 15 minutes.
-- Anomalies are flagged when z-score > 1.5 (configurable in the Workflow job).
--
-- SQL Alert and Workflow integration:
--   1. Create a Databricks SQL Alert pointing at grizl_recent_anomaly_signals.
--      Alert condition: "Query returns at least 1 row."
--      Action: POST to /api/databricks/incidents (the backend incident webhook).
--      Alternative: run notebooks/04_anomaly_signals_workflow.py on a 5-minute
--      Workflow schedule for richer webhook payloads with full anomaly rows.
--
-- Lakehouse Monitoring complement:
--   Lakehouse Monitoring (notebooks/02_lakehouse_monitor_setup.py) generates
--   grizl.observability_monitors.raw_logs_drift_metrics, which contains managed
--   drift detection for error_rate, http_5xx_rate, and p95_duration_ms.
--   SQL Alerts can also be built on those drift metric tables.
--
-- Requires base views from sql/grizl-observability.sql:
--   grizl.observability.http_requests
--   grizl.observability.application_errors
--   grizl.observability.deployments
--   grizl.observability.forwarder_health
-- =============================================================================


-- ── BackendHttpErrorRateAnomalies ─────────────────────────────────────────────
-- Z-score anomalies in backend HTTP 5xx/error rate by service and route.
-- Lookback: 2 days. Detection window: last 15 minutes. Min requests: 20.

CREATE OR REPLACE VIEW grizl.observability.backend_http_error_rate_anomalies
  COMMENT 'Positive z-score anomalies in backend HTTP 5xx/error rate. Equivalent to KQL BackendHttpErrorRateAnomalies().'
AS
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300)        AS time_bin,
    service,
    route,
    COUNT(*)                                                                        AS requests,
    SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR', 'CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) AS error_rate
  FROM grizl.observability.http_requests
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
    AND service IS NOT NULL
    AND route IS NOT NULL
  GROUP BY 1, 2, 3
),
baseline_stats AS (
  SELECT service, route,
    AVG(error_rate)    AS baseline_mean,
    STDDEV(error_rate) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1, 2
)
SELECT
  ts.time_bin                                                                        AS ingest_timestamp,
  'Backend HTTP Error Rate Anomaly'                                                  AS alert_name,
  CASE WHEN ts.error_rate >= 0.25 OR ts.errors >= 10 THEN 'critical' ELSE 'error' END AS severity,
  'APPLICATION_ERROR'                                                                AS anomaly_type,
  'backend_http_error_rate'                                                          AS signal_name,
  ts.service,
  ts.route,
  ROUND((ts.error_rate - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0), 3)      AS anomaly_score,
  ROUND(bs.baseline_mean, 4)                                                         AS baseline,
  ROUND(ts.error_rate, 4)                                                            AS actual,
  ROUND(bs.baseline_mean, 4)                                                         AS expected,
  ts.requests                                                                        AS total,
  ts.errors,
  ts.time_bin                                                                        AS time_window_start,
  ts.time_bin + INTERVAL 5 MINUTES                                                   AS time_window_end,
  named_struct('service', ts.service, 'route', ts.route)                             AS dimensions,
  'BackendHttpErrorRateAnomalies'                                                    AS sql_view,
  'sql/grizl-anomaly-signals.sql#BackendHttpErrorRateAnomalies'                     AS sql_query_ref
FROM time_series ts
JOIN baseline_stats bs ON ts.service = bs.service AND ts.route = bs.route
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND ts.requests >= 20
  AND (ts.error_rate - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) > 1.5
ORDER BY anomaly_score DESC;


-- ── RouteLatencyAnomalies ─────────────────────────────────────────────────────
-- Z-score anomalies in route p95 latency. Returns no rows when duration_ms is not populated.

CREATE OR REPLACE VIEW grizl.observability.route_latency_anomalies
  COMMENT 'Positive z-score anomalies in route p95 latency. Equivalent to KQL RouteLatencyAnomalies().'
AS
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service,
    route,
    COUNT(*)                              AS requests,
    PERCENTILE(duration_ms, 0.95)         AS p95_ms
  FROM grizl.observability.http_requests
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
    AND service IS NOT NULL
    AND route IS NOT NULL
    AND duration_ms IS NOT NULL
    AND duration_ms >= 0
  GROUP BY 1, 2, 3
),
baseline_stats AS (
  SELECT service, route,
    AVG(p95_ms)    AS baseline_mean,
    STDDEV(p95_ms) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1, 2
)
SELECT
  ts.time_bin                                                                      AS ingest_timestamp,
  'Route Latency Anomaly'                                                          AS alert_name,
  CASE WHEN ts.p95_ms >= 5000 THEN 'critical' ELSE 'warning' END                  AS severity,
  'HIGH_LATENCY'                                                                   AS anomaly_type,
  'route_latency_p95'                                                              AS signal_name,
  ts.service,
  ts.route,
  ROUND((ts.p95_ms - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0), 3)        AS anomaly_score,
  ROUND(bs.baseline_mean, 2)                                                       AS baseline,
  ROUND(ts.p95_ms, 2)                                                              AS actual,
  ROUND(bs.baseline_mean, 2)                                                       AS expected,
  ts.requests                                                                      AS total,
  ts.time_bin                                                                      AS time_window_start,
  ts.time_bin + INTERVAL 5 MINUTES                                                 AS time_window_end,
  named_struct('service', ts.service, 'route', ts.route, 'metric', 'p95_duration_ms') AS dimensions,
  'RouteLatencyAnomalies'                                                          AS sql_view,
  'sql/grizl-anomaly-signals.sql#RouteLatencyAnomalies'                           AS sql_query_ref
FROM time_series ts
JOIN baseline_stats bs ON ts.service = bs.service AND ts.route = bs.route
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND ts.requests >= 20
  AND ts.p95_ms >= 1000
  AND (ts.p95_ms - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) > 1.5
ORDER BY anomaly_score DESC;


-- ── ErrorSignatureSpikeAnomalies ──────────────────────────────────────────────
-- Z-score anomalies in grouped application error signatures.

CREATE OR REPLACE VIEW grizl.observability.error_signature_spike_anomalies
  COMMENT 'Positive z-score anomalies in error signature occurrence counts. Equivalent to KQL ErrorSignatureSpikeAnomalies().'
AS
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service,
    route,
    error_type,
    error_signature,
    COUNT(*) AS occurrences
  FROM grizl.observability.application_errors
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
    AND service IS NOT NULL
    AND error_signature IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),
baseline_stats AS (
  SELECT service, error_type, error_signature,
    AVG(occurrences)    AS baseline_mean,
    STDDEV(occurrences) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1, 2, 3
)
SELECT
  ts.time_bin                                                                        AS ingest_timestamp,
  'Error Signature Spike'                                                            AS alert_name,
  CASE WHEN ts.occurrences >= 10 THEN 'critical' ELSE 'error' END                   AS severity,
  'APPLICATION_ERROR'                                                                AS anomaly_type,
  'error_signature_spike'                                                            AS signal_name,
  ts.service,
  ts.route,
  ts.error_type,
  ts.error_signature,
  ROUND((ts.occurrences - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0), 3)     AS anomaly_score,
  ROUND(bs.baseline_mean, 2)                                                         AS baseline,
  CAST(ts.occurrences AS DOUBLE)                                                     AS actual,
  ROUND(bs.baseline_mean, 2)                                                         AS expected,
  ts.time_bin                                                                        AS time_window_start,
  ts.time_bin + INTERVAL 5 MINUTES                                                   AS time_window_end,
  named_struct('service', ts.service, 'route', ts.route,
               'error_type', ts.error_type,
               'error_signature', ts.error_signature)                                AS dimensions,
  'ErrorSignatureSpikeAnomalies'                                                     AS sql_view,
  'sql/grizl-anomaly-signals.sql#ErrorSignatureSpikeAnomalies'                      AS sql_query_ref
FROM time_series ts
JOIN baseline_stats bs ON ts.service = bs.service
  AND ts.error_type = bs.error_type
  AND ts.error_signature = bs.error_signature
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND ts.occurrences >= 3
  AND (ts.occurrences - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) > 1.5
ORDER BY anomaly_score DESC;


-- ── ForwarderFreshnessDropAnomalies ───────────────────────────────────────────
-- Negative z-score anomalies in forwarder healthy event volume (drop detection).

CREATE OR REPLACE VIEW grizl.observability.forwarder_freshness_drop_anomalies
  COMMENT 'Negative anomalies in forwarder healthy event volume. Equivalent to KQL ForwarderFreshnessDropAnomalies().'
AS
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service,
    COUNT(*) AS events
  FROM grizl.observability.forwarder_health
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
    AND event_type IN ('forwarder_start', 'batch_sent', 'batch_acked', 'health_check', 'ready_check')
  GROUP BY 1, 2
),
baseline_stats AS (
  SELECT service,
    AVG(events)    AS baseline_mean,
    STDDEV(events) AS baseline_stddev,
    MAX(events)    AS baseline_max
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1
),
last_seen AS (
  SELECT service, MAX(ingest_timestamp) AS last_event_at
  FROM grizl.observability.forwarder_health
  WHERE event_type IN ('forwarder_start', 'batch_sent', 'batch_acked', 'health_check', 'ready_check')
  GROUP BY 1
)
SELECT
  ts.time_bin                                                                       AS ingest_timestamp,
  'Forwarder Freshness Drop'                                                        AS alert_name,
  'warning'                                                                         AS severity,
  'FORWARDER_STALE'                                                                 AS anomaly_type,
  'forwarder_freshness_drop'                                                        AS signal_name,
  ts.service,
  ROUND(ABS((ts.events - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0)), 3)   AS anomaly_score,
  ROUND(bs.baseline_mean, 2)                                                        AS baseline,
  CAST(ts.events AS DOUBLE)                                                         AS actual,
  ROUND(bs.baseline_mean, 2)                                                        AS expected,
  ls.last_event_at                                                                  AS last_seen,
  CAST(
    (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(ls.last_event_at)) / 60
  AS INT)                                                                           AS minutes_since_last_seen,
  ts.time_bin                                                                       AS time_window_start,
  ts.time_bin + INTERVAL 5 MINUTES                                                  AS time_window_end,
  named_struct('service', ts.service, 'last_seen', CAST(ls.last_event_at AS STRING)) AS dimensions,
  'ForwarderFreshnessDropAnomalies'                                                 AS sql_view,
  'sql/grizl-anomaly-signals.sql#ForwarderFreshnessDropAnomalies'                  AS sql_query_ref
FROM time_series ts
JOIN baseline_stats bs ON ts.service = bs.service
JOIN last_seen ls ON ts.service = ls.service
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND (
    (ts.events < bs.baseline_mean
     AND (bs.baseline_mean - ts.events) / NULLIF(bs.baseline_stddev, 0) > 1.5)
    OR (ts.events = 0 AND bs.baseline_mean >= 1)
  )
ORDER BY anomaly_score DESC;


-- ── ForwarderDropFailureAnomalies ─────────────────────────────────────────────
-- Positive z-score anomalies in forwarder skipped/failure event counts.

CREATE OR REPLACE VIEW grizl.observability.forwarder_drop_failure_anomalies
  COMMENT 'Positive anomalies in forwarder skipped/failure events. Equivalent to KQL ForwarderDropFailureAnomalies().'
AS
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service,
    event_type,
    COUNT(*) AS failures
  FROM grizl.observability.forwarder_health
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
    AND (
      event_type IN ('message_skipped', 'batch_retry', 'batch_nack', 'batch_failed')
      OR severity IN ('ERROR', 'CRITICAL')
    )
  GROUP BY 1, 2, 3
),
baseline_stats AS (
  SELECT service, event_type,
    AVG(failures)    AS baseline_mean,
    STDDEV(failures) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1, 2
)
SELECT
  ts.time_bin                                                                       AS ingest_timestamp,
  'Forwarder Drop/Failure Anomaly'                                                  AS alert_name,
  CASE WHEN ts.failures >= 5 THEN 'error' ELSE 'warning' END                       AS severity,
  'FORWARDER_FAILURE'                                                               AS anomaly_type,
  'forwarder_drop_failure_spike'                                                    AS signal_name,
  ts.service,
  ts.event_type,
  ROUND((ts.failures - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0), 3)       AS anomaly_score,
  ROUND(bs.baseline_mean, 2)                                                        AS baseline,
  CAST(ts.failures AS DOUBLE)                                                       AS actual,
  ROUND(bs.baseline_mean, 2)                                                        AS expected,
  ts.time_bin                                                                       AS time_window_start,
  ts.time_bin + INTERVAL 5 MINUTES                                                  AS time_window_end,
  named_struct('service', ts.service, 'event_type', ts.event_type)                 AS dimensions,
  'ForwarderDropFailureAnomalies'                                                   AS sql_view,
  'sql/grizl-anomaly-signals.sql#ForwarderDropFailureAnomalies'                    AS sql_query_ref
FROM time_series ts
JOIN baseline_stats bs ON ts.service = bs.service AND ts.event_type = bs.event_type
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND ts.failures > 0
  AND (ts.failures - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) > 1.5
ORDER BY anomaly_score DESC;


-- ── PostDeploymentRegressionAnomalies ─────────────────────────────────────────
-- Error-rate anomalies that occur within 2 hours of a deployment event.

CREATE OR REPLACE VIEW grizl.observability.post_deployment_regression_anomalies
  COMMENT 'Service error-rate anomalies occurring shortly after a deployment SHA. Equivalent to KQL PostDeploymentRegressionAnomalies().'
AS
WITH deployment_bins AS (
  SELECT service, deployment_sha, MIN(ingest_timestamp) AS deployed_at
  FROM grizl.observability.deployments
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND service IS NOT NULL
    AND deployment_sha IS NOT NULL
  GROUP BY 1, 2
),
time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service,
    COUNT(*)                                                                   AS requests,
    SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR', 'CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) AS error_rate
  FROM grizl.observability.http_requests
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 7 DAYS
    AND service IS NOT NULL
  GROUP BY 1, 2
),
baseline_stats AS (
  SELECT service,
    AVG(error_rate)    AS baseline_mean,
    STDDEV(error_rate) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 30 MINUTES
  GROUP BY 1
),
anomalies AS (
  SELECT ts.time_bin, ts.service, ts.requests, ts.errors, ts.error_rate,
    bs.baseline_mean, bs.baseline_stddev,
    (ts.error_rate - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) AS z_score
  FROM time_series ts
  JOIN baseline_stats bs ON ts.service = bs.service
  WHERE ts.time_bin >= current_timestamp() - INTERVAL 30 MINUTES
    AND ts.requests >= 20
    AND (ts.error_rate - bs.baseline_mean) / NULLIF(bs.baseline_stddev, 0) > 1.5
)
SELECT
  a.time_bin                                                                         AS ingest_timestamp,
  'Post-Deployment Regression Anomaly'                                               AS alert_name,
  CASE WHEN a.error_rate >= 0.25 OR a.errors >= 10 THEN 'critical' ELSE 'error' END AS severity,
  'POST_DEPLOYMENT_ERROR'                                                             AS anomaly_type,
  'post_deployment_regression'                                                        AS signal_name,
  a.service,
  d.deployment_sha,
  d.deployed_at,
  ROUND(a.z_score, 3)                                                                AS anomaly_score,
  ROUND(a.baseline_mean, 4)                                                          AS baseline,
  ROUND(a.error_rate, 4)                                                             AS actual,
  ROUND(a.baseline_mean, 4)                                                          AS expected,
  a.requests                                                                         AS total,
  a.errors,
  a.time_bin                                                                         AS time_window_start,
  a.time_bin + INTERVAL 5 MINUTES                                                    AS time_window_end,
  named_struct('service', a.service,
               'deployment_sha', d.deployment_sha,
               'deployed_at', CAST(d.deployed_at AS STRING))                         AS dimensions,
  'PostDeploymentRegressionAnomalies'                                                AS sql_view,
  'sql/grizl-anomaly-signals.sql#PostDeploymentRegressionAnomalies'                 AS sql_query_ref
FROM anomalies a
JOIN deployment_bins d ON a.service = d.service
WHERE a.time_bin BETWEEN d.deployed_at AND d.deployed_at + INTERVAL 2 HOURS
ORDER BY anomaly_score DESC;


-- ── GrizlRecentAnomalySignals ─────────────────────────────────────────────────
-- Union of all anomaly signal views — primary target for SQL Alerts and the
-- Workflow anomaly job (notebooks/04_anomaly_signals_workflow.py).

CREATE OR REPLACE VIEW grizl.observability.grizl_recent_anomaly_signals
  COMMENT 'Union of all GRIZL anomaly signal views for SQL Alert and Workflow trigger. Equivalent to KQL GrizlRecentAnomalySignals().'
AS
SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, NULL AS route, NULL AS error_type, NULL AS error_signature,
  NULL AS deployment_sha, NULL AS deployed_at,
  anomaly_score, baseline, actual, expected,
  total, errors,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.backend_http_error_rate_anomalies

UNION ALL

SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, route, NULL, NULL, NULL, NULL,
  anomaly_score, baseline, actual, expected,
  total, NULL AS errors,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.route_latency_anomalies

UNION ALL

SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, route, error_type, error_signature, NULL, NULL,
  anomaly_score, baseline, actual, expected,
  NULL AS total, NULL AS errors,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.error_signature_spike_anomalies

UNION ALL

SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, NULL, NULL, NULL, NULL, NULL,
  anomaly_score, baseline, actual, expected,
  NULL, NULL,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.forwarder_freshness_drop_anomalies

UNION ALL

SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, NULL, NULL, NULL, NULL, NULL,
  anomaly_score, baseline, actual, expected,
  NULL, NULL,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.forwarder_drop_failure_anomalies

UNION ALL

SELECT
  ingest_timestamp, alert_name, severity, anomaly_type, signal_name,
  service, NULL, NULL, NULL, deployment_sha, deployed_at,
  anomaly_score, baseline, actual, expected,
  total, errors,
  time_window_start, time_window_end, TO_JSON(dimensions) AS dimensions, sql_view, sql_query_ref
FROM grizl.observability.post_deployment_regression_anomalies

ORDER BY anomaly_score DESC, ingest_timestamp DESC;
