#!/usr/bin/env bash
# Manage the Snowflake account-level network policy (CIS Benchmark 3.1).
# Satisfies: SECURITY_ESSENTIALS_CIS3_1 — all users covered by a network policy.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash security/snowflake/scripts/network-policy.sh [options] <command>

Commands:
  apply     Create or replace the account-level network policy and assign it
  verify    Show the current network policy and recent login history
  remove    Drop the network policy and clear the account assignment

USAGE
  usage_common
}

COMMAND=

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)  [ "$#" -ge 2 ] || die "--config requires a path"; CONFIG_FILE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --yes)     YES=true; shift ;;
    apply|verify|remove) COMMAND="$1"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done

[ -n "${COMMAND}" ] || { usage; die "A command (apply|verify|remove) is required."; }

load_env_file
require_snow

cmd_apply() {
  require_env SNOWFLAKE_NETWORK_POLICY_NAME
  require_env SNOWFLAKE_TRUSTED_IPS

  local ip_list
  ip_list="$(build_ip_list "${SNOWFLAKE_TRUSTED_IPS}")"

  info "Policy name : ${SNOWFLAKE_NETWORK_POLICY_NAME}"
  info "Trusted IPs : ${SNOWFLAKE_TRUSTED_IPS}"

  ensure_mutation_allowed

  snow_sql "
CREATE OR REPLACE NETWORK POLICY ${SNOWFLAKE_NETWORK_POLICY_NAME}
  ALLOWED_IP_LIST = (${ip_list})
  COMMENT = 'GRIZL trusted IP policy — CIS Benchmark 3.1 (SECURITY_ESSENTIALS_CIS3_1)';

ALTER ACCOUNT SET NETWORK_POLICY = ${SNOWFLAKE_NETWORK_POLICY_NAME};
"
  ok "Account-level network policy applied: ${SNOWFLAKE_NETWORK_POLICY_NAME}"

  # Optional: separate CI/CD policy assigned to a specific service user
  if [ -n "${SNOWFLAKE_CICD_POLICY_NAME:-}" ] && \
     [ -n "${SNOWFLAKE_CICD_TRUSTED_IPS:-}" ] && \
     [ -n "${SNOWFLAKE_CICD_USER:-}" ]; then

    local cicd_ip_list
    cicd_ip_list="$(build_ip_list "${SNOWFLAKE_CICD_TRUSTED_IPS}")"

    info "Creating CI/CD policy ${SNOWFLAKE_CICD_POLICY_NAME} for user ${SNOWFLAKE_CICD_USER}"
    snow_sql "
CREATE OR REPLACE NETWORK POLICY ${SNOWFLAKE_CICD_POLICY_NAME}
  ALLOWED_IP_LIST = (${cicd_ip_list})
  COMMENT = 'GRIZL CI/CD service account policy — overrides account policy for ${SNOWFLAKE_CICD_USER}';

ALTER USER ${SNOWFLAKE_CICD_USER} SET NETWORK_POLICY = ${SNOWFLAKE_CICD_POLICY_NAME};
"
    ok "CI/CD policy applied to user: ${SNOWFLAKE_CICD_USER}"
  fi
}

cmd_verify() {
  info "Account-level network policy parameter:"
  snow_sql "SHOW PARAMETERS LIKE 'NETWORK_POLICY' IN ACCOUNT;"

  info "All network policies in account:"
  snow_sql "SHOW NETWORK POLICIES;"

  info "Active users and their network policy overrides:"
  snow_sql "
SELECT name, login_name, disabled, last_success_login
FROM snowflake.account_usage.users
WHERE deleted_on IS NULL
ORDER BY last_success_login DESC NULLS LAST;"

  info "Recent login IPs (last 7 days):"
  snow_sql "
SELECT user_name, client_ip, reported_client_type, COUNT(*) AS logins,
       MAX(event_timestamp) AS last_seen
FROM snowflake.account_usage.login_history
WHERE event_timestamp > DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND client_ip != '0.0.0.0'
GROUP BY 1, 2, 3
ORDER BY last_seen DESC;"
}

cmd_remove() {
  require_env SNOWFLAKE_NETWORK_POLICY_NAME

  warn "This will remove the network policy and leave the account unrestricted."
  ensure_mutation_allowed

  snow_sql "
ALTER ACCOUNT UNSET NETWORK_POLICY;
DROP NETWORK POLICY IF EXISTS ${SNOWFLAKE_NETWORK_POLICY_NAME};
"
  ok "Network policy removed."

  if [ -n "${SNOWFLAKE_CICD_POLICY_NAME:-}" ] && [ -n "${SNOWFLAKE_CICD_USER:-}" ]; then
    snow_sql "
ALTER USER ${SNOWFLAKE_CICD_USER} UNSET NETWORK_POLICY;
DROP NETWORK POLICY IF EXISTS ${SNOWFLAKE_CICD_POLICY_NAME};
"
    ok "CI/CD policy removed."
  fi
}

case "${COMMAND}" in
  apply)  cmd_apply  ;;
  verify) cmd_verify ;;
  remove) cmd_remove ;;
esac
