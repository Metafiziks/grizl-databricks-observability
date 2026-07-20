#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

parse_common_args "$@"
load_env_file

info "Checking required tools"
require_cmd databricks
require_cmd curl
require_cmd node
ok "Required tools found"

info "Checking Databricks CLI version"
databricks --version
ok "Databricks CLI version logged"

info "Checking Databricks authentication"
require_databricks_auth

info "Checking current Databricks identity"
if [ "${DRY_RUN}" != "true" ]; then
  databricks current-user me
fi
ok "Auth check passed"

info "Checking required env vars"
require_env DATABRICKS_HOST
require_env DATABRICKS_WAREHOUSE_ID
ok "Required env vars set"

if [ -n "${DATABRICKS_CATALOG:-}" ]; then
  info "Checking catalog: ${DATABRICKS_CATALOG}"
  if [ "${DRY_RUN}" != "true" ]; then
    databricks catalogs get "${DATABRICKS_CATALOG}" >/dev/null 2>&1 \
      && ok "Catalog '${DATABRICKS_CATALOG}' exists" \
      || warn "Catalog '${DATABRICKS_CATALOG}' not found — run provision.sh to create it"
  fi
fi

if [ -n "${DATABRICKS_WAREHOUSE_ID:-}" ] && [ "${DRY_RUN}" != "true" ]; then
  info "Checking SQL warehouse: ${DATABRICKS_WAREHOUSE_ID}"
  databricks sql warehouses get "${DATABRICKS_WAREHOUSE_ID}" >/dev/null 2>&1 \
    && ok "SQL warehouse '${DATABRICKS_WAREHOUSE_ID}' exists" \
    || warn "SQL warehouse '${DATABRICKS_WAREHOUSE_ID}' not found — create one in Databricks SQL"
fi

ok "Preflight checks passed"
