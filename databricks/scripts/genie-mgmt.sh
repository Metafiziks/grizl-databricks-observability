#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash databricks/scripts/genie-mgmt.sh [options] <action>

Actions:
  create        Create the GRIZL Observability Genie space and print its space_id
  list          List all Genie spaces in the workspace
  get           Get details of DATABRICKS_GENIE_SPACE_ID
  ask <prompt>  Send a question to the Genie space and print the answer
                (smoke-test for the incident evidence flow)

The Genie space is created via:
  bash databricks/scripts/genie-mgmt.sh create
Then set DATABRICKS_GENIE_SPACE_ID in grizl.databricks.env.

Required config:
  DATABRICKS_HOST
  DATABRICKS_GENIE_SPACE_ID (for get/ask actions)

Authentication:
  Uses 'databricks auth token'. Run 'databricks auth login' before live use.

USAGE
  usage_common
}

ACTION=""
QUESTION=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      [ "$#" -ge 2 ] || die "--config requires a path"
      CONFIG_FILE="$2"; shift 2 ;;
    --dry-run)  DRY_RUN=true; shift ;;
    --yes)      YES=true; shift ;;
    create|list|get) ACTION="$1"; shift ;;
    ask)
      [ "$#" -ge 2 ] || die "ask requires a question argument"
      ACTION="ask"; QUESTION="$2"; shift 2 ;;
    -h|--help)  usage; exit 0 ;;
    *)          die "Unknown option: $1" ;;
  esac
done

[ -n "${ACTION}" ] || { usage; exit 1; }

load_env_file

if [ "${DRY_RUN}" != "true" ]; then
  require_databricks
fi
require_env DATABRICKS_HOST

HOST="${DATABRICKS_HOST%/}"

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
  create)
    require_env DATABRICKS_WAREHOUSE_ID
    info "Creating GRIZL Observability Genie space"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd databricks genie create-space \
        "${DATABRICKS_WAREHOUSE_ID}" "<serialized_space>" \
        --title "GRIZL Observability" \
        --parent-path "/Users/<workspace-user>" \
        --profile grizl
      exit 0
    fi
    ensure_mutation_allowed

    SERIALIZED="$(node -e "
const crypto = require('crypto');
const uuid = () => crypto.randomBytes(16).toString('hex');
const tables = [
  'grizl.observability.application_errors',
  'grizl.observability.backend_http_error_rate_anomalies',
  'grizl.observability.error_signature_spike_anomalies',
  'grizl.observability.grizl_recent_anomaly_signals',
  'grizl.observability.http_requests',
  'grizl.observability.raw_logs',
  'grizl.observability.route_latency_anomalies'
].sort();
const space = {
  version: 2,
  config: {
    sample_questions: [
      { id: uuid(), question: ['What are the top anomalies in the last 15 minutes?'] },
      { id: uuid(), question: ['Which services have the highest error rates right now?'] },
      { id: uuid(), question: ['Show me routes with latency spikes above 2 standard deviations.'] },
      { id: uuid(), question: ['Are there any POST deployment errors for recent deployments?'] }
    ]
  },
  data_sources: { tables: tables.map(id => ({ identifier: id })) },
  instructions: {
    text_instructions: [{
      id: uuid(),
      content: [
        '* anomaly_score is a z-score: above 2 is significant, above 3 is critical.\n',
        '* detection_window_minutes is 15; baseline_window_hours is 48.\n',
        '* deployment_sha links anomalies to a specific git commit.\n',
        '* service matches grizl-backend, grizl-frontend, or grizl-log-forwarder.\n',
        '* For current anomalies, filter ts within the last 15 minutes.\n',
        '* error_signature is the normalized error class without variable parts.\n'
      ]
    }]
  }
};
console.log(JSON.stringify(space, null, 2));
")"

    WORKSPACE_USER="$(databricks api get /api/2.0/preview/scim/v2/Me \
      --profile "${DATABRICKS_PROFILE:-grizl}" -o json \
      | node -e "const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(d.userName||'')")"

    databricks genie create-space \
      "${DATABRICKS_WAREHOUSE_ID}" \
      "${SERIALIZED}" \
      --title "GRIZL Observability" \
      --description "AI/BI evidence agent for GRIZL infrastructure anomaly signals." \
      --parent-path "/Users/${WORKSPACE_USER}" \
      --profile "${DATABRICKS_PROFILE:-grizl}" -o json \
      | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
console.log('space_id:', d.space_id);
console.log('title:   ', d.title);
console.log();
console.log('Add to grizl.databricks.env:');
console.log('  DATABRICKS_GENIE_SPACE_ID=' + d.space_id);
"
    ;;

  list)
    info "Listing Genie spaces in ${HOST}"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS "${HOST}/api/2.0/genie/spaces" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    call_api GET "/api/2.0/genie/spaces" \
      | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
const spaces = d.genie_spaces || d.spaces || d || [];
(Array.isArray(spaces) ? spaces : [spaces]).forEach(s => {
  console.log(s.space_id + '  ' + (s.title || '(untitled)'));
});
"
    ;;

  get)
    require_env DATABRICKS_GENIE_SPACE_ID
    SPACE_ID="${DATABRICKS_GENIE_SPACE_ID}"
    info "Getting Genie space ${SPACE_ID}"
    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS "${HOST}/api/2.0/genie/spaces/${SPACE_ID}" \
        -H "Authorization: Bearer <token>"
      exit 0
    fi
    call_api GET "/api/2.0/genie/spaces/${SPACE_ID}" | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
console.log(JSON.stringify(d, null, 2));
"
    ;;

  ask)
    require_env DATABRICKS_GENIE_SPACE_ID
    SPACE_ID="${DATABRICKS_GENIE_SPACE_ID}"
    info "Sending question to Genie space ${SPACE_ID}"
    info "Question: ${QUESTION}"

    if [ "${DRY_RUN}" = "true" ]; then
      quote_cmd curl -sS \
        "${HOST}/api/2.0/genie/spaces/${SPACE_ID}/start-conversation" \
        -H "Authorization: Bearer <token>" \
        -H "Content-Type: application/json" \
        -d "{\"content\": \"${QUESTION}\"}"
      exit 0
    fi

    body="$(QUESTION="${QUESTION}" node -e "console.log(JSON.stringify({content: process.env.QUESTION}))")"
    response="$(call_api POST "/api/2.0/genie/spaces/${SPACE_ID}/start-conversation" "${body}")"
    CONV_ID="$(echo "${response}" | node -e "const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(d.conversation_id||'');")"
    MSG_ID="$(echo "${response}"  | node -e "const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(d.message_id||(d.message&&d.message.id)||'');")"
    info "conversation_id: ${CONV_ID}"
    info "message_id:      ${MSG_ID}"

    info "Polling for answer (up to 90s)..."
    for i in $(seq 1 30); do
      sleep 3
      poll="$(call_api GET "/api/2.0/genie/spaces/${SPACE_ID}/conversations/${CONV_ID}/messages/${MSG_ID}")"
      status="$(echo "${poll}" | node -e "const d=JSON.parse(require('fs').readFileSync('/dev/stdin','utf8')); console.log(d.status||(d.message&&d.message.status)||'');")"
      if [ "${status}" = "COMPLETED" ] || [ "${status}" = "FAILED" ]; then
        echo "${poll}" | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
console.log('Status:', d.status||(d.message&&d.message.status));
const atts = (d.attachments||[]).concat(d.message&&d.message.attachments||[]);
atts.forEach(a => {
  if (a.text)  console.log('Answer:', a.text.content||a.text);
  if (a.query) console.log('SQL:   ', a.query.query||a.query.statement);
});
"
        break
      fi
      info "  (${i}) status=${status}, waiting..."
    done
    ;;
esac

ok "genie-mgmt finished."
