#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib.sh
. "${SCRIPT_DIR}/lib.sh"

usage() {
  cat <<'USAGE'
Usage: bash databricks/scripts/sql-exec.sh [options]

Executes a SQL statement against a Databricks SQL warehouse via the
Statements REST API (/api/2.0/sql/statements).

Options:
  --statement <sql>  SQL statement text
  --file <path>      Read SQL from a file (sent as a single multi-statement request)
  --catalog <name>   Default catalog for the statement (overrides DATABRICKS_CATALOG)
  --schema <name>    Default schema for the statement (overrides DATABRICKS_SCHEMA)

Required config:
  DATABRICKS_HOST
  DATABRICKS_WAREHOUSE_ID

Authentication:
  Uses 'databricks auth token' to obtain the current workspace token.
  Run 'databricks auth login --host <host>' before live use.

USAGE
  usage_common
}

SQL_STATEMENT=""
SQL_FILE=""
CATALOG_OVERRIDE=""
SCHEMA_OVERRIDE=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --config)
      [ "$#" -ge 2 ] || die "--config requires a path"
      CONFIG_FILE="$2"; shift 2 ;;
    --statement)
      [ "$#" -ge 2 ] || die "--statement requires a value"
      SQL_STATEMENT="$2"; shift 2 ;;
    --file)
      [ "$#" -ge 2 ] || die "--file requires a path"
      SQL_FILE="$2"; shift 2 ;;
    --catalog)
      [ "$#" -ge 2 ] || die "--catalog requires a value"
      CATALOG_OVERRIDE="$2"; shift 2 ;;
    --schema)
      [ "$#" -ge 2 ] || die "--schema requires a value"
      SCHEMA_OVERRIDE="$2"; shift 2 ;;
    --dry-run)
      DRY_RUN=true; shift ;;
    --yes)
      YES=true; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      die "Unknown option: $1" ;;
  esac
done

load_env_file

if [ -n "${SQL_STATEMENT}" ] && [ -n "${SQL_FILE}" ]; then
  die "Use either --statement or --file, not both"
fi
if [ -z "${SQL_STATEMENT}" ] && [ -z "${SQL_FILE}" ]; then
  die "A SQL statement is required via --statement or --file"
fi

if [ -z "${DATABRICKS_HOST:-}" ] && [ "${DRY_RUN}" = "true" ]; then
  DATABRICKS_HOST="https://<DATABRICKS_HOST>"
else
  require_env DATABRICKS_HOST
fi

if [ -z "${DATABRICKS_WAREHOUSE_ID:-}" ] && [ "${DRY_RUN}" = "true" ]; then
  DATABRICKS_WAREHOUSE_ID="{DATABRICKS_WAREHOUSE_ID}"
else
  require_env DATABRICKS_WAREHOUSE_ID
fi

EFFECTIVE_CATALOG="${CATALOG_OVERRIDE:-${DATABRICKS_CATALOG:-grizl}}"
EFFECTIVE_SCHEMA="${SCHEMA_OVERRIDE:-${DATABRICKS_SCHEMA:-observability}}"

ensure_mutation_allowed

if [ -n "${SQL_FILE}" ]; then
  case "${SQL_FILE}" in
    /*) ;;
    *) SQL_FILE="${REPO_ROOT}/${SQL_FILE}" ;;
  esac
  [ -f "${SQL_FILE}" ] || die "SQL file not found: ${SQL_FILE}"
  # Split file into individual statements and run each one
  split_dir="$(mktemp -d)"
  trap 'rm -rf "${split_dir}"' EXIT
  node -e "
const fs = require('fs');
const path = require('path');
const src = fs.readFileSync(process.argv[1], 'utf8');
const dir = process.argv[2];
// Split on semicolons, strip line comments, trim
const stmts = src.split(';')
  .map(raw => raw.replace(/--[^\n]*/g, '').trim())
  .filter(s => s.length > 0);
stmts.forEach((s, i) => {
  fs.writeFileSync(path.join(dir, String(i).padStart(4, '0') + '.sql'), s + ';');
});
console.log(stmts.length);
" "${SQL_FILE}" "${split_dir}"
  stmt_count="$(ls "${split_dir}"/*.sql 2>/dev/null | wc -l | tr -d ' ')"
  info "Executing ${stmt_count} statements from ${SQL_FILE}"
  for stmt_file in "${split_dir}"/*.sql; do
    stmt_label="$(head -1 "${stmt_file}" | cut -c1-80)"
    info "  ${stmt_label}"
    export SQL_STATEMENT DATABRICKS_WAREHOUSE_ID EFFECTIVE_CATALOG EFFECTIVE_SCHEMA
    SQL_STATEMENT="$(cat "${stmt_file}")"
    payload_file="$(mktemp)"
    write_json_payload "${payload_file}" '{
      warehouse_id:     process.env.DATABRICKS_WAREHOUSE_ID,
      statement:        process.env.SQL_STATEMENT,
      catalog:          process.env.EFFECTIVE_CATALOG,
      schema:           process.env.EFFECTIVE_SCHEMA,
      wait_timeout:     "50s",
      on_wait_timeout:  "CANCEL",
      disposition:      "INLINE"
    }'
    if [ "${DRY_RUN}" = "true" ]; then
      echo "  databricks api post /api/2.0/sql/statements --json @<payload>"
      cat "${payload_file}"
      rm -f "${payload_file}"
      continue
    fi
    response="$(databricks_api_post "/api/2.0/sql/statements" "${payload_file}")"
    rm -f "${payload_file}"
    echo "${response}" | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
const state = d.status && d.status.state;
if (state === 'SUCCEEDED') {
  console.log('[OK]    succeeded');
} else if (state === 'FAILED') {
  const msg = (d.status && d.status.error && d.status.error.message) || JSON.stringify(d);
  process.stderr.write('[ERROR] failed: ' + msg + '\n');
  process.exit(1);
} else {
  process.stderr.write('[WARN]  Unexpected: ' + JSON.stringify(d) + '\n');
}
"
  done
  exit 0
fi

payload_file="$(mktemp)"
trap 'rm -f "${payload_file:-}"' EXIT

export SQL_STATEMENT DATABRICKS_WAREHOUSE_ID EFFECTIVE_CATALOG EFFECTIVE_SCHEMA
write_json_payload "${payload_file}" '{
  warehouse_id:     process.env.DATABRICKS_WAREHOUSE_ID,
  statement:        process.env.SQL_STATEMENT,
  catalog:          process.env.EFFECTIVE_CATALOG,
  schema:           process.env.EFFECTIVE_SCHEMA,
  wait_timeout:     "50s",
  on_wait_timeout:  "CANCEL",
  disposition:      "INLINE"
}'

if [ "${DRY_RUN}" = "true" ]; then
  echo "databricks api post /api/2.0/sql/statements --json @<payload>"
  cat "${payload_file}"
  exit 0
fi

require_cmd databricks

response="$(databricks_api_post "/api/2.0/sql/statements" "${payload_file}")"

echo "${response}" | node -e "
const d = JSON.parse(require('fs').readFileSync('/dev/stdin', 'utf8'));
const state = d.status && d.status.state;
if (state === 'SUCCEEDED') {
  const rows = (d.result && d.result.data_array) || [];
  const cols = (d.manifest && d.manifest.schema && d.manifest.schema.columns) || [];
  const count = (d.result && d.result.row_count) || rows.length;
  console.log('[OK]    SQL statement succeeded (' + count + ' row(s))');
  if (rows.length > 0 && rows.length <= 50) {
    if (cols.length > 0) {
      console.log(cols.map(c => c.name).join('\t'));
      console.log(cols.map(() => '------').join('\t'));
    }
    rows.forEach(row => console.log((row || []).join('\t')));
  }
} else if (state === 'FAILED') {
  const msg = (d.status && d.status.error && d.status.error.message) || JSON.stringify(d);
  process.stderr.write('[ERROR] SQL statement failed: ' + msg + '\n');
  process.exit(1);
} else {
  console.log(JSON.stringify(d, null, 2));
}
"
