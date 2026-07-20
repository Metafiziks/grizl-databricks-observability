#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash databricks/scripts/provision.sh [options] [targets]

Targets:
  --all           Provision all resources
  --catalog       Create the Unity Catalog catalog
  --schema        Create the observability and observability_monitors schemas
  --table         Create the raw_logs Delta table (run after DLT pipeline if using DLT)
  --volume        Create the Unity Catalog Volume for Auto Loader checkpoints

This script provisions the Databricks Unity Catalog layer. The DLT Auto Loader
pipeline (notebooks/01_autoloader_pipeline.py) manages the raw_logs table once
deployed via 'databricks bundle deploy'. Run --table only for manual bootstrapping
without DLT.

USAGE
  usage_common
}

RUN_CATALOG=false
RUN_SCHEMA=false
RUN_TABLE=false
RUN_VOLUME=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      [ "$#" -ge 2 ] || die "--config requires a path"
      CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)   DRY_RUN=true; shift ;;
    --yes)       YES=true; shift ;;
    --all)       RUN_CATALOG=true; RUN_SCHEMA=true; RUN_VOLUME=true; shift ;;
    --catalog)   RUN_CATALOG=true; shift ;;
    --schema)    RUN_SCHEMA=true; shift ;;
    --table)     RUN_TABLE=true; shift ;;
    --volume)    RUN_VOLUME=true; shift ;;
    -h|--help)   usage; exit 0 ;;
    *)           die "Unknown option: $1" ;;
  esac
done

if [ "${RUN_CATALOG}" = "false" ] && [ "${RUN_SCHEMA}" = "false" ] && \
   [ "${RUN_TABLE}" = "false" ]   && [ "${RUN_VOLUME}" = "false" ]; then
  RUN_CATALOG=true
  RUN_SCHEMA=true
  RUN_VOLUME=true
fi

load_env_file

if [ "${DRY_RUN}" != "true" ]; then
  require_databricks
fi
require_databricks_auth
ensure_mutation_allowed

CATALOG="${DATABRICKS_CATALOG:-grizl}"
SCHEMA="${DATABRICKS_SCHEMA:-observability}"
WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-}"

run_sql() {
  local stmt="$1"
  local label="${2:-SQL}"
  local dry_arg="" yes_arg=""
  [ "${DRY_RUN}" = "true" ] && dry_arg="--dry-run"
  [ "${YES}"     = "true" ] && yes_arg="--yes"
  info "${label}"
  bash "${SCRIPT_DIR}/sql-exec.sh" \
    --config "${CONFIG_FILE}" \
    --statement "${stmt}" \
    ${dry_arg} ${yes_arg}
}

if [ "${RUN_CATALOG}" = "true" ]; then
  [ -n "${WAREHOUSE_ID}" ] || [ "${DRY_RUN}" = "true" ] || require_env DATABRICKS_WAREHOUSE_ID
  run_sql "CREATE CATALOG IF NOT EXISTS ${CATALOG}" "Creating catalog '${CATALOG}'"
fi

if [ "${RUN_SCHEMA}" = "true" ]; then
  run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.${SCHEMA} \
    COMMENT 'GRIZL application observability — raw logs, views, and anomaly signals.'" \
    "Creating schema '${CATALOG}.${SCHEMA}'"

  run_sql "CREATE SCHEMA IF NOT EXISTS ${CATALOG}.observability_monitors \
    COMMENT 'Lakehouse Monitoring output tables for ${CATALOG}.${SCHEMA}.raw_logs.'" \
    "Creating schema '${CATALOG}.observability_monitors'"
fi

if [ "${RUN_VOLUME}" = "true" ]; then
  run_sql "CREATE VOLUME IF NOT EXISTS ${CATALOG}.${SCHEMA}.checkpoints \
    COMMENT 'Auto Loader schema checkpoint storage for the DLT pipeline.'" \
    "Creating Unity Catalog Volume '${CATALOG}.${SCHEMA}.checkpoints'"
fi

if [ "${RUN_TABLE}" = "true" ]; then
  warn "Creating raw_logs table manually. Prefer deploying notebooks/01_autoloader_pipeline.py as a DLT pipeline."
  run_sql "$(cat "${REPO_ROOT}/sql/grizl-observability.sql" \
    | sed -n '/^CREATE TABLE IF NOT EXISTS/,/^);$/p' | head -40)" \
    "Creating raw_logs Delta table"
fi

ok "Provision script finished."
ok "Next steps:"
ok "  1. Deploy the DLT Auto Loader pipeline: databricks bundle deploy --target dev"
ok "  2. Apply SQL views:                     npm --prefix databricks run sql:observability"
ok "  3. Apply anomaly signal views:          npm --prefix databricks run sql:anomaly-signals"
ok "  4. Run Lakehouse Monitor setup:         Open notebooks/02_lakehouse_monitor_setup.py in Databricks"
