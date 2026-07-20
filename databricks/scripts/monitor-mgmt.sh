#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash databricks/scripts/monitor-mgmt.sh [options] <action>

Actions:
  get             Get the Lakehouse Monitor for DATABRICKS_MONITOR_TABLE
  refresh         Trigger an immediate monitor refresh
  list-runs       List recent monitor refresh runs
  delete          Delete the monitor (does not drop output tables)

The monitor is created by running notebooks/02_lakehouse_monitor_setup.py
in the Databricks workspace. Use this script to inspect or trigger refreshes
from the command line.

Required config:
  DATABRICKS_HOST
  DATABRICKS_MONITOR_TABLE   (default: grizl.observability.raw_logs)

Authentication:
  Uses 'databricks auth token'. Run 'databricks auth login' before live use.

USAGE
  usage_common
}

ACTION=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      [ "$#" -ge 2 ] || die "--config requires a path"
      CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)           DRY_RUN=true; shift ;;
    --yes)               YES=true; shift ;;
    get|refresh|list-runs|delete) ACTION="$1"; shift ;;
    -h|--help)           usage; exit 0 ;;
    *)                   die "Unknown option: $1" ;;
  esac
done

[ -n "${ACTION}" ] || { usage; exit 1; }

load_env_file
require_env DATABRICKS_HOST
HOST="${DATABRICKS_HOST%/}"
TABLE="${DATABRICKS_MONITOR_TABLE:-grizl.observability.raw_logs}"
ENCODED_TABLE="$(node -e "console.log(encodeURIComponent(process.env.TABLE))" TABLE="${TABLE}")"

call_api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local method_lc tmp_file
  method_lc="$(echo "${method}" | tr '[:upper:]' '[:lower:]')"
  if [ -n "${body}" ]; then
    tmp_file="$(mktemp)"
    printf '%s' "${body}" > "${tmp_file}"
    databricks api "${method_lc}" "${path}" --json "@${tmp_file}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
    rm -f "${tmp_file}"
  else
    databricks api "${method_lc}" "${path}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
  fi
}

case "${ACTION}" in
  get)
    info "Getting Lakehouse Monitor for ${TABLE}"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS "${HOST}/api/2.0/quality-monitors/${ENCODED_TABLE}" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    call_api GET "/api/2.0/quality-monitors/${ENCODED_TABLE}" \
      | node -e "const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(JSON.stringify(d, null, 2));"
    ;;

  refresh)
    info "Triggering monitor refresh for ${TABLE}"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS -X POST \
        "${HOST}/api/2.0/quality-monitors/${ENCODED_TABLE}/refresh" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    ensure_mutation_allowed
    call_api POST "/api/2.0/quality-monitors/${ENCODED_TABLE}/refresh" "" \
      | node -e "const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(JSON.stringify(d, null, 2));"
    ;;

  list-runs)
    info "Listing recent monitor refresh runs for ${TABLE}"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS \
        "${HOST}/api/2.0/quality-monitors/${ENCODED_TABLE}/runs" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    call_api GET "/api/2.0/quality-monitors/${ENCODED_TABLE}/runs" \
      | node -e "const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(JSON.stringify(d, null, 2));"
    ;;

  delete)
    warn "This will delete the monitor config for ${TABLE} but will NOT drop the output tables."
    ensure_mutation_allowed
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS -X DELETE \
        "${HOST}/api/2.0/quality-monitors/${ENCODED_TABLE}" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    call_api DELETE "/api/2.0/quality-monitors/${ENCODED_TABLE}" "" \
      | node -e "const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(JSON.stringify(d, null, 2));"
    ;;
esac

ok "monitor-mgmt finished."
