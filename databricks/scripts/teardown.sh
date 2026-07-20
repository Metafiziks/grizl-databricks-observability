#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

parse_common_args "$@"
load_env_file

if [ "${DRY_RUN}" != "true" ]; then
  require_databricks
fi
require_databricks_auth
require_env DATABRICKS_HOST
ensure_mutation_allowed

HOST="${DATABRICKS_HOST%/}"
CATALOG="${DATABRICKS_CATALOG:-grizl}"

run_sql() {
  local stmt="$1"
  local label="${2:-SQL}"
  info "${label}"
  bash "${SCRIPT_DIR}/sql-exec.sh" \
    --config "${CONFIG_FILE}" \
    --statement "${stmt}" \
    ${DRY_RUN:+--dry-run} \
    ${YES:+--yes}
}

# Drop anomaly signal views
for view in \
  grizl_recent_anomaly_signals \
  post_deployment_regression_anomalies \
  forwarder_drop_failure_anomalies \
  forwarder_freshness_drop_anomalies \
  error_signature_spike_anomalies \
  route_latency_anomalies \
  backend_http_error_rate_anomalies; do
  run_sql "DROP VIEW IF EXISTS ${CATALOG}.observability.${view}" \
    "Dropping view ${CATALOG}.observability.${view}"
done

# Drop base views
for view in forwarder_health deployments frontend_telemetry application_errors http_requests; do
  run_sql "DROP VIEW IF EXISTS ${CATALOG}.observability.${view}" \
    "Dropping view ${CATALOG}.observability.${view}"
done

# Drop the ML anomaly scores table if it was created by notebook 03
run_sql "DROP TABLE IF EXISTS ${CATALOG}.observability.ml_anomaly_scores" \
  "Dropping table ${CATALOG}.observability.ml_anomaly_scores"

if [ "${DATABRICKS_DROP_RAW_LOGS:-false}" = "true" ]; then
  warn "Dropping raw_logs Delta table — this deletes all ingested log data."
  run_sql "DROP TABLE IF EXISTS ${CATALOG}.observability.raw_logs" \
    "Dropping table ${CATALOG}.observability.raw_logs"
else
  warn "raw_logs table retained. Set DATABRICKS_DROP_RAW_LOGS=true to drop it."
fi

if [ "${DATABRICKS_DROP_SCHEMAS:-false}" = "true" ]; then
  run_sql "DROP SCHEMA IF EXISTS ${CATALOG}.observability_monitors CASCADE" \
    "Dropping schema ${CATALOG}.observability_monitors"
  run_sql "DROP SCHEMA IF EXISTS ${CATALOG}.observability CASCADE" \
    "Dropping schema ${CATALOG}.observability"
else
  warn "Schemas retained. Set DATABRICKS_DROP_SCHEMAS=true to drop schemas."
fi

if [ "${DATABRICKS_DROP_CATALOG:-false}" = "true" ]; then
  warn "Dropping catalog ${CATALOG} — this drops ALL schemas and tables inside it."
  run_sql "DROP CATALOG IF EXISTS ${CATALOG} CASCADE" \
    "Dropping catalog ${CATALOG}"
else
  warn "Catalog retained. Set DATABRICKS_DROP_CATALOG=true to drop it."
fi

ok "Teardown finished."
ok "Remaining cleanup (done in the Databricks UI):"
ok "  - Delete the DLT Auto Loader pipeline"
ok "  - Delete the Workflow anomaly-signals job"
ok "  - Delete SQL Alerts built on the anomaly signal views"
ok "  - Delete the Genie space"
ok "  - Delete the Lakehouse Monitor (databricks/scripts/monitor-mgmt.sh delete)"
