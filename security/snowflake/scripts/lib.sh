#!/usr/bin/env bash

if [ -n "${GRIZL_SNOWFLAKE_SECURITY_LIB_SOURCED:-}" ]; then
  return 0
fi
GRIZL_SNOWFLAKE_SECURITY_LIB_SOURCED=1

SECURITY_SF_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECURITY_SF_DIR="$(cd "${SECURITY_SF_SCRIPT_DIR}/.." && pwd)"

CONFIG_FILE="${CONFIG_FILE:-${SECURITY_SF_DIR}/config/grizl.snowflake.security.env}"
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
                   (default: security/snowflake/config/grizl.snowflake.security.env)
  --dry-run        Print SQL without executing it
  --yes            Required for live create/alter/drop operations
USAGE
}

parse_common_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --config)
        [ "$#" -ge 2 ] || die "--config requires a path"
        CONFIG_FILE="$2"; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      --yes)     YES=true; shift ;;
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
    warn "Copy security/snowflake/config/grizl.snowflake.security.env.example to grizl.snowflake.security.env"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

require_snow() {
  require_cmd snow
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
  [ "${YES}" = "true" ] || die "Live Snowflake mutations require --yes. Re-run with --dry-run first."
}

# Run a SQL string against Snowflake.
# Writes to a temp file so multi-statement SQL works cleanly.
snow_sql() {
  local sql="$1"
  require_snow
  require_env SNOWFLAKE_CONNECTION

  local tmpfile
  tmpfile="$(mktemp /tmp/grizl-sf-security-XXXXX.sql)"
  trap 'rm -f "${tmpfile}"' RETURN

  printf '%s\n' "${sql}" > "${tmpfile}"

  if [ "${DRY_RUN}" = "true" ]; then
    info "[dry-run] Would execute:"
    printf '%s\n' "${sql}"
  else
    snow sql -c "${SNOWFLAKE_CONNECTION}" --filename "${tmpfile}"
  fi
}

# Convert a comma-separated IP list to Snowflake list literal: 'a', 'b', 'c'
build_ip_list() {
  local raw="$1"
  printf '%s' "${raw}" \
    | tr ',' '\n' \
    | sed "s/^[[:space:]]*//; s/[[:space:]]*$//" \
    | grep -v '^$' \
    | awk '{printf "%s'"'"'%s'"'"'", (NR>1 ? ", " : ""), $0}' \
    | sed 's/^, //'
}
