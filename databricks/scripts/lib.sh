#!/usr/bin/env bash

if [ -n "${GRIZL_DATABRICKS_LIB_SOURCED:-}" ]; then
  return 0
fi
GRIZL_DATABRICKS_LIB_SOURCED=1

DATABRICKS_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATABRICKS_DIR="$(cd "${DATABRICKS_SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DATABRICKS_DIR}/.." && pwd)"

CONFIG_FILE="${CONFIG_FILE:-${DATABRICKS_DIR}/config/grizl.databricks.env}"
DRY_RUN="${DRY_RUN:-false}"
YES="${YES:-false}"

info() { printf '[INFO]  %s\n' "$*"; }
ok()   { printf '[OK]    %s\n' "$*"; }
warn() { printf '[WARN]  %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage_common() {
  cat <<'USAGE'
Common options:
  --config <path>  Load env vars from a config file (default: databricks/config/grizl.databricks.env)
  --dry-run        Print API calls without executing them
  --yes            Required for live create/delete operations
USAGE
}

parse_common_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --config)
        [ "$#" -ge 2 ] || die "--config requires a path"
        CONFIG_FILE="$2"; shift 2 ;;
      --dry-run)
        DRY_RUN=true; shift ;;
      --yes)
        YES=true; shift ;;
      *) return 0 ;;
    esac
  done
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
    warn "Copy databricks/config/grizl.databricks.env.example to databricks/config/grizl.databricks.env"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_databricks() {
  require_cmd databricks
}

require_databricks_auth() {
  if [ "${DRY_RUN}" = "true" ]; then
    info "Dry-run mode: skipping Databricks auth check."
    return 0
  fi

  if databricks auth status >/dev/null 2>&1; then
    ok "Databricks CLI authentication detected."
    return 0
  fi

  die "Databricks CLI not authenticated. Run 'databricks auth login --host ${DATABRICKS_HOST:-<host>}' then retry."
}

require_env() {
  local name="$1"
  eval "local value=\${${name}:-}"
  [ -n "${value}" ] || die "${name} must be set"
}

ensure_mutation_allowed() {
  if [ "${DRY_RUN}" = "true" ]; then
    return 0
  fi
  [ "${YES}" = "true" ] || die "Live Databricks mutations require --yes. Re-run with --dry-run first."
}

quote_cmd() {
  local first=true
  for arg in "$@"; do
    [ "${first}" = "true" ] && first=false || printf ' '
    printf '%q' "${arg}"
  done
  printf '\n'
}

# databricks_api_post <path> <json-file>
# databricks_api_get  <path>
# Use 'databricks api' instead of extracting a raw token with 'databricks auth token'.
# The CLI handles OAuth internally — no token is materialised in the shell.
databricks_api_post() {
  local path="$1"
  local json_file="$2"
  require_cmd databricks
  databricks api post "${path}" --json "@${json_file}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}

databricks_api_get() {
  local path="$1"
  require_cmd databricks
  databricks api get "${path}" --profile "${DATABRICKS_PROFILE:-grizl}" -o json
}

write_json_payload() {
  local output_file="$1"
  local js_expression="$2"
  require_cmd node
  node -e "const fs = require('fs'); const p = ${js_expression}; fs.writeFileSync(process.argv[1], JSON.stringify(p, null, 2) + '\n');" "${output_file}"
}
