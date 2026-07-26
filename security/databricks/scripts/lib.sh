#!/usr/bin/env bash

if [ -n "${GRIZL_DATABRICKS_SECURITY_LIB_SOURCED:-}" ]; then
  return 0
fi
GRIZL_DATABRICKS_SECURITY_LIB_SOURCED=1

SECURITY_DB_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECURITY_DB_DIR="$(cd "${SECURITY_DB_SCRIPT_DIR}/.." && pwd)"

CONFIG_FILE="${CONFIG_FILE:-${SECURITY_DB_DIR}/config/grizl.databricks.security.env}"
DRY_RUN="${DRY_RUN:-false}"
YES="${YES:-false}"

info() { printf '[INFO]  %s\n' "$*"; }
ok()   { printf '[OK]    %s\n' "$*"; }
warn() { printf '[WARN]  %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage_common() {
  cat <<'USAGE'
Common options:
  --config <path>  Load env vars from a config file
                   (default: security/databricks/config/grizl.databricks.security.env)
  --dry-run        Print API calls without executing them
  --yes            Required for live create/update/delete operations
USAGE
}

load_env_file() {
  if [ -f "${CONFIG_FILE}" ]; then
    info "Loading config ${CONFIG_FILE}"
    set -a
    # shellcheck source=/dev/null
    . "${CONFIG_FILE}"
    set +a
  else
    warn "Config file not found: ${CONFIG_FILE}"
    warn "Copy security/databricks/config/grizl.databricks.security.env.example to grizl.databricks.security.env"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_databricks() {
  require_cmd databricks
}

require_env() {
  local name="$1"
  eval "local value=\${${name}:-}"
  [ -n "${value}" ] || die "${name} must be set in your config file"
}

ensure_mutation_allowed() {
  if [ "${DRY_RUN}" = "true" ]; then
    return 0
  fi
  [ "${YES}" = "true" ] || die "Live Databricks mutations require --yes. Re-run with --dry-run first."
}

# Build a JSON array of IP strings from a comma-separated env var.
# e.g. "1.2.3.4/32,10.0.0.0/8" → ["1.2.3.4/32","10.0.0.0/8"]
build_ip_json_array() {
  local raw="$1"
  node -e "
const ips = process.env._IPS.split(',').map(s => s.trim()).filter(Boolean);
process.stdout.write(JSON.stringify(ips));
" _IPS="${raw}"
}

# POST/PATCH to Databricks REST API using the CLI (no token materialised).
databricks_api_post() {
  local path="$1"
  local json_file="$2"
  databricks api post "${path}" --json "@${json_file}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}

databricks_api_get() {
  local path="$1"
  databricks api get "${path}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}

databricks_api_patch() {
  local path="$1"
  local json_file="$2"
  databricks api patch "${path}" --json "@${json_file}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}

databricks_api_delete() {
  local path="$1"
  databricks api delete "${path}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}
