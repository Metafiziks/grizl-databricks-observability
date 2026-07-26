#!/usr/bin/env bash
# Manage the Databricks workspace IP Access List.
# Restricts workspace UI and REST API access to trusted IP ranges.
#
# Prerequisites:
#   1. IP Access Lists must be enabled on the workspace:
#      Admin Console → Security → IP Access Lists → Enable
#   2. Databricks CLI authenticated: databricks auth login --host <host>
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash security/databricks/scripts/ip-access-list.sh [options] <command>

Commands:
  apply     Create or replace the ALLOW IP access list for the workspace
  verify    Show current IP access lists and enabled state
  remove    Delete the ALLOW list (leaves workspace open if no other lists exist)

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
require_databricks

cmd_apply() {
  require_env DATABRICKS_HOST
  require_env DATABRICKS_IP_LIST_LABEL
  require_env DATABRICKS_TRUSTED_IPS

  local ip_array
  ip_array="$(build_ip_json_array "${DATABRICKS_TRUSTED_IPS}")"

  info "List label : ${DATABRICKS_IP_LIST_LABEL}"
  info "Trusted IPs: ${DATABRICKS_TRUSTED_IPS}"

  ensure_mutation_allowed

  # Check if a list with this label already exists
  local existing_id
  existing_id="$(
    databricks_api_get /api/2.0/ip-access-lists 2>/dev/null \
      | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
const lists = d.ip_access_lists || [];
const match = lists.find(l => l.label === process.env.LABEL);
process.stdout.write(match ? match.list_id : '');
" LABEL="${DATABRICKS_IP_LIST_LABEL}" \
      || true
  )"

  local tmpfile
  tmpfile="$(mktemp /tmp/grizl-db-security-XXXXX.json)"
  trap 'rm -f "${tmpfile}"' RETURN

  if [ -n "${existing_id}" ]; then
    info "Updating existing list (id: ${existing_id})"
    node -e "
const fs = require('fs');
const payload = {
  label: process.env.LABEL,
  list_type: 'ALLOW',
  ip_addresses: JSON.parse(process.env.IPS),
  enabled: true
};
fs.writeFileSync(process.env.OUT, JSON.stringify(payload, null, 2) + '\n');
" LABEL="${DATABRICKS_IP_LIST_LABEL}" IPS="${ip_array}" OUT="${tmpfile}"

    if [ "${DRY_RUN}" = "true" ]; then
      info "[dry-run] PATCH /api/2.0/ip-access-lists/${existing_id}"
      cat "${tmpfile}"
    else
      databricks_api_patch "/api/2.0/ip-access-lists/${existing_id}" "${tmpfile}"
      ok "IP access list updated."
    fi
  else
    info "Creating new ALLOW list"
    node -e "
const fs = require('fs');
const payload = {
  label: process.env.LABEL,
  list_type: 'ALLOW',
  ip_addresses: JSON.parse(process.env.IPS)
};
fs.writeFileSync(process.env.OUT, JSON.stringify(payload, null, 2) + '\n');
" LABEL="${DATABRICKS_IP_LIST_LABEL}" IPS="${ip_array}" OUT="${tmpfile}"

    if [ "${DRY_RUN}" = "true" ]; then
      info "[dry-run] POST /api/2.0/ip-access-lists"
      cat "${tmpfile}"
    else
      databricks_api_post /api/2.0/ip-access-lists "${tmpfile}"
      ok "IP access list created."
    fi
  fi
}

cmd_verify() {
  info "IP Access List feature status:"
  databricks_api_get /api/2.0/workspace-conf \
    | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
console.log('enableIpAccessLists:', d.enableIpAccessLists ?? '(not set)');
" || warn "Could not read workspace config."

  info "Current IP access lists:"
  databricks_api_get /api/2.0/ip-access-lists \
    | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
const lists = d.ip_access_lists || [];
if (lists.length === 0) { console.log('  (none)'); process.exit(0); }
for (const l of lists) {
  console.log(\`  [\${l.list_type}] \${l.label} (id: \${l.list_id}, enabled: \${l.enabled})\`);
  (l.ip_addresses || []).forEach(ip => console.log(\`    \${ip}\`));
}
"
}

cmd_remove() {
  require_env DATABRICKS_IP_LIST_LABEL

  warn "This will remove the ALLOW list. If IP access lists remain enabled with no ALLOW list, access may be blocked entirely."
  ensure_mutation_allowed

  local existing_id
  existing_id="$(
    databricks_api_get /api/2.0/ip-access-lists 2>/dev/null \
      | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
const lists = d.ip_access_lists || [];
const match = lists.find(l => l.label === process.env.LABEL);
process.stdout.write(match ? match.list_id : '');
" LABEL="${DATABRICKS_IP_LIST_LABEL}" \
      || true
  )"

  if [ -z "${existing_id}" ]; then
    warn "No list found with label '${DATABRICKS_IP_LIST_LABEL}'. Nothing to remove."
    return 0
  fi

  if [ "${DRY_RUN}" = "true" ]; then
    info "[dry-run] DELETE /api/2.0/ip-access-lists/${existing_id}"
  else
    databricks_api_delete "/api/2.0/ip-access-lists/${existing_id}"
    ok "IP access list removed (id: ${existing_id})."
  fi
}

case "${COMMAND}" in
  apply)  cmd_apply  ;;
  verify) cmd_verify ;;
  remove) cmd_remove ;;
esac
