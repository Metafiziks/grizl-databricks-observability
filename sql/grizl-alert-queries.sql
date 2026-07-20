-- =============================================================================
-- GRIZL Databricks SQL Alert Trigger Queries — grizl.observability
-- =============================================================================
-- How to use (simple threshold alerts):
--   1. Open Databricks SQL → Alerts → Create alert.
--   2. Select a SQL query from this file (or paste it as a new query).
--   3. Set alert condition: "Value" column >= threshold, or "Query returns rows."
--   4. Set check interval as noted per alert.
--   5. Add a notification destination:
--      - For the incident orchestrator: use the Webhook destination type.
--        URL: https://<backend-host>/api/databricks/incidents
--        Custom payload template: see webhook payload template below.
--      - For PagerDuty, Slack, or email: use the built-in integrations.
--   6. Save and enable.
--
-- NOTE: Databricks SQL Alert webhook payloads contain alert metadata but not
-- query result rows. For rich payloads with full anomaly context, use the
-- Databricks Workflow job (notebooks/04_anomaly_signals_workflow.py) instead.
-- The Workflow job runs on a 5-minute schedule and POSTs a complete anomaly
-- payload equivalent to the Fabric Activator webhook output.
--
-- Requires views from sql/grizl-observability.sql and sql/grizl-anomaly-signals.sql.
-- All base views must exist before these alert queries can be saved.
-- =============================================================================


-- ── ALERT 1: Backend HTTP Error Spike ────────────────────────────────────────
-- Alert name:     Backend HTTP Error Spike
-- Description:    Fires when the 5xx/ERROR/CRITICAL rate across a service
--                 exceeds 10% over the last 10 minutes with at least 20 requests.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.
-- Threshold:      requests >= 20 AND error_rate >= 0.10 (10%)

SELECT service,
       COUNT(*)                                                                    AS requests,
       SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR','CRITICAL') THEN 1 ELSE 0 END) AS errors,
       SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR','CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) AS error_rate
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
GROUP BY 1
HAVING COUNT(*) >= 20
  AND SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR','CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) >= 0.10
ORDER BY error_rate DESC;


-- ── ALERT 2: Frontend API Error Spike ────────────────────────────────────────
-- Alert name:     Frontend API Error Spike
-- Description:    Fires when browser/API error events from grizl-frontend reach
--                 5 or more occurrences for the same page+error_type pair within
--                 the last 10 minutes.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.

SELECT page, error_type, COUNT(*) AS errors
FROM grizl.observability.frontend_telemetry
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
  AND (event_type = 'api_error' OR severity IN ('ERROR', 'CRITICAL'))
GROUP BY 1, 2
HAVING COUNT(*) >= 5
ORDER BY errors DESC;


-- ── ALERT 3: Repeated Application Error Signature ────────────────────────────
-- Alert name:     Repeated Application Error Signature
-- Description:    Fires when the same backend error_signature occurs 3 or more
--                 times within the last 15 minutes.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.

SELECT service, error_type, error_signature, COUNT(*) AS occurrences
FROM grizl.observability.application_errors
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 15 MINUTES
  AND error_signature IS NOT NULL
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY occurrences DESC;


-- ── ALERT 4: Post-Deployment Error Increase ──────────────────────────────────
-- Alert name:     Post-Deployment Error Increase
-- Description:    Fires when a specific deployment_sha accumulates 3 or more
--                 ERROR/CRITICAL events in the last 30 minutes.
-- Check interval: Every 10 minutes
-- Condition:      Query returns at least 1 row.

SELECT deployment_sha, service,
       COUNT(*) AS total,
       SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) AS errors
FROM grizl.observability.raw_logs
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 30 MINUTES
  AND deployment_sha IS NOT NULL
GROUP BY 1, 2
HAVING SUM(CASE WHEN severity IN ('ERROR', 'CRITICAL') THEN 1 ELSE 0 END) >= 3
ORDER BY errors DESC;


-- ── ALERT 5: Forwarder Stale — No Recent Health Events ───────────────────────
-- Alert name:     Forwarder Stale
-- Description:    Fires when no forwarder_health event has been seen in the
--                 last 10 minutes, indicating the grizl-log-forwarder is
--                 down or has lost its Pub/Sub subscription.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.

SELECT service,
       MAX(ingest_timestamp)                                              AS last_seen,
       CAST(
         (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(ingest_timestamp))) / 60
       AS INT)                                                           AS minutes_since_last_seen
FROM grizl.observability.forwarder_health
WHERE event_type IN ('forwarder_start', 'batch_sent', 'batch_acked', 'health_check', 'ready_check')
GROUP BY 1
HAVING CAST(
  (UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(ingest_timestamp))) / 60
AS INT) > 10;


-- ── ALERT 6: Forwarder Skipped / Failure Events ──────────────────────────────
-- Alert name:     Forwarder Failure Events
-- Description:    Fires when the log forwarder emits any message_skipped,
--                 batch_retry, batch_nack, or batch_failed events, or any
--                 ERROR/CRITICAL events, within the last 15 minutes.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.

SELECT event_type, severity, COUNT(*) AS cnt
FROM grizl.observability.forwarder_health
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 15 MINUTES
  AND (
    event_type IN ('message_skipped', 'batch_retry', 'batch_nack', 'batch_failed')
    OR severity IN ('ERROR', 'CRITICAL')
  )
GROUP BY 1, 2
HAVING COUNT(*) > 0;


-- ── ALERT 7: Active Anomaly Signals (Union) ───────────────────────────────────
-- Alert name:     GRIZL Anomaly Signal
-- Description:    Fires when any of the z-score anomaly views detects a signal.
--                 Use this as a single catch-all alert backed by the Workflow job
--                 for richer webhook payloads, or as a standalone SQL alert for
--                 quick setup.
-- Check interval: Every 5 minutes
-- Condition:      Query returns at least 1 row.
-- NOTE:           This query scans 2 days of data. For high-volume tables,
--                 prefer the Workflow job which is optimized for incremental runs.

SELECT alert_name, severity, anomaly_type, signal_name, service,
       anomaly_score, actual, baseline, sql_view
FROM grizl.observability.grizl_recent_anomaly_signals
ORDER BY anomaly_score DESC
LIMIT 10;


-- ── ALERT 8: Lakehouse Monitoring — Error Rate Drift ─────────────────────────
-- Alert name:     Error Rate Drift (Lakehouse Monitor)
-- Description:    Fires when Lakehouse Monitoring detects statistically significant
--                 drift in the error_rate custom metric. Requires that the monitor
--                 has run at least one refresh cycle.
-- Check interval: Every 1 hour (aligned to monitor refresh schedule)
-- Condition:      Query returns at least 1 row.
-- Requires:       notebooks/02_lakehouse_monitor_setup.py to have run.

-- SELECT window_start_time, slice_key, slice_value,
--        error_rate, error_rate_delta, drift_type
-- FROM grizl.observability_monitors.raw_logs_drift_metrics
-- WHERE column_name = 'error_rate'
--   AND window_start_time >= current_timestamp() - INTERVAL 2 HOURS
--   AND drift_type IS NOT NULL;


-- =============================================================================
-- WEBHOOK PAYLOAD TEMPLATE
-- Configure this custom body template in the Databricks SQL Alert webhook
-- destination. The {{ALERT_*}} and {{QUERY_*}} tokens are resolved by Databricks.
--
-- For richer payloads containing full anomaly rows, use the Workflow job
-- (notebooks/04_anomaly_signals_workflow.py) instead of SQL Alert webhooks.
-- =============================================================================

-- {
--   "alertName":   "{{ALERT_NAME}}",
--   "alertState":  "{{ALERT_STATE}}",
--   "alertUrl":    "{{ALERT_URL}}",
--   "service":     "grizl",
--   "severity":    "ERROR",
--   "query":       "{{QUERY_NAME}}",
--   "value":       "{{VALUE}}",
--   "timestamp":   "{{TIMESTAMP}}"
-- }


-- =============================================================================
-- VALIDATION QUERIES
-- Run these to confirm alert queries return the expected results.
-- =============================================================================

-- Alert 1 validation: no rows when backend is healthy
SELECT service, COUNT(*) AS requests,
  SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR','CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) AS error_rate
FROM grizl.observability.http_requests
WHERE ingest_timestamp >= current_timestamp() - INTERVAL 10 MINUTES
GROUP BY 1
HAVING COUNT(*) >= 20
  AND SUM(CASE WHEN status_code >= 500 OR severity IN ('ERROR','CRITICAL') THEN 1.0 ELSE 0.0 END) / COUNT(*) >= 0.10;

-- Alert 5 validation: no rows when forwarder is running
SELECT service, MAX(ingest_timestamp) AS last_seen,
  CAST((UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(ingest_timestamp))) / 60 AS INT) AS minutes_since_last_seen
FROM grizl.observability.forwarder_health
WHERE event_type IN ('forwarder_start', 'batch_sent', 'batch_acked', 'health_check', 'ready_check')
GROUP BY 1
HAVING CAST((UNIX_TIMESTAMP(current_timestamp()) - UNIX_TIMESTAMP(MAX(ingest_timestamp))) / 60 AS INT) > 10;
