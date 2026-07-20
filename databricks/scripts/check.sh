#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

info "Checking shell syntax"
for script in "${DATABRICKS_DIR}"/scripts/*.sh; do
  bash -n "${script}"
done
ok "Shell syntax passed"

info "Checking JSON manifests"
for json_file in "${DATABRICKS_DIR}"/package.json "${DATABRICKS_DIR}"/manifests/*.json; do
  node -e "const fs = require('fs'); JSON.parse(fs.readFileSync(process.argv[1], 'utf8'));" "${json_file}"
done
ok "JSON manifests parsed"

info "Checking referenced SQL source files"
for sql_file in \
  "${REPO_ROOT}/sql/grizl-observability.sql" \
  "${REPO_ROOT}/sql/grizl-anomaly-signals.sql" \
  "${REPO_ROOT}/sql/grizl-dashboard-tiles.sql" \
  "${REPO_ROOT}/sql/grizl-alert-queries.sql"; do
  [ -f "${sql_file}" ] || die "Missing SQL source file: ${sql_file}"
done
ok "SQL source files exist"

info "Checking referenced notebook files"
for nb_file in \
  "${REPO_ROOT}/notebooks/01_autoloader_pipeline.py" \
  "${REPO_ROOT}/notebooks/02_lakehouse_monitor_setup.py" \
  "${REPO_ROOT}/notebooks/03_anomaly_model_example.py" \
  "${REPO_ROOT}/notebooks/04_anomaly_signals_workflow.py"; do
  [ -f "${nb_file}" ] || die "Missing notebook file: ${nb_file}"
done
ok "Notebook files exist"

info "Checking for committed credential material"
if find "${DATABRICKS_DIR}" -type f \
    ! -path "${DATABRICKS_DIR}/scripts/check.sh" \
    -print0 \
  | xargs -0 grep -E \
    'dapi[0-9a-f]{32}|DATABRICKS_TOKEN=.+[a-zA-Z0-9]|client_secret=.+[a-zA-Z0-9]' \
    >/dev/null 2>&1; then
  die "Potential Databricks token or client secret found under databricks/"
fi
ok "No credential material detected"

ok "Databricks package local checks passed"
