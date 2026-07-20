# Databricks notebook source
# MAGIC %md
# MAGIC # GRIZL Anomaly Signals Workflow Job
# MAGIC
# MAGIC Runs as a Databricks Workflow task on a 5-minute schedule.
# MAGIC Queries `grizl.observability.grizl_recent_anomaly_signals` (the union of all
# MAGIC z-score anomaly views), collects direct SQL evidence from Delta tables, and
# MAGIC creates a GitHub issue for each anomaly batch — optionally assigning Copilot
# MAGIC for code-actionable incidents.
# MAGIC
# MAGIC Mirrors the Fabric Incident Orchestrator pattern
# MAGIC (`services/fabricIncidentOrchestrator.service.js`) with the following
# MAGIC substitutions:
# MAGIC
# MAGIC | Fabric | Databricks |
# MAGIC |---|---|
# MAGIC | Fabric Activator trigger | Databricks Workflow (5-min cron) |
# MAGIC | Fabric Data Agent (MCP) | Direct Spark SQL on Delta tables |
# MAGIC | KQL (Kusto) fallback | Additional Spark SQL queries |
# MAGIC | Entra client credentials | Databricks OAuth M2M (for Genie, if used) |
# MAGIC
# MAGIC **Infrastructure anomalies only.** Chat-quality anomalies
# MAGIC (FALLBACK_RESPONSE, HIGH_LATENCY on the agent, RETRIEVAL_NOT_RUN)
# MAGIC are handled by the LangGraph agent runtime — not this workflow.
# MAGIC
# MAGIC ## Parameters (set in the Workflow task definition)
# MAGIC
# MAGIC | Parameter | Description |
# MAGIC |---|---|
# MAGIC | `score_threshold` | Minimum z-score to include (default: 1.5) |
# MAGIC | `dry_run` | Print payload, skip GitHub API calls (default: false) |
# MAGIC | `github_token` | GitHub PAT with `issues:write` permission |
# MAGIC | `repo_map_json` | JSON object mapping service name → `owner/repo` |
# MAGIC | `github_fallback_repo` | Repo to use when service is not in `repo_map_json` |
# MAGIC | `copilot_enabled` | Assign Copilot to code-actionable issues (default: true) |
# MAGIC | `copilot_assignee` | GitHub username for Copilot handoff (default: Copilot) |

# COMMAND ----------

import json
import re
import time as _time_module
import urllib.request
import urllib.error
from datetime import datetime, timezone
try:
    import mlflow
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print("[WARN] mlflow not available — skipping experiment tracking")

# ── Widget parameters ─────────────────────────────────────────────────────────
dbutils.widgets.text("score_threshold",      "1.5",     "Anomaly Score Threshold")
dbutils.widgets.text("dry_run",              "false",   "Dry Run (true/false)")
dbutils.widgets.text("github_token",         "",        "GitHub Token (issues:write)")
dbutils.widgets.text("repo_map_json",        "{}",      "Service→Repo Map JSON")
dbutils.widgets.text("github_fallback_repo", "",        "Fallback Repo (owner/repo)")
dbutils.widgets.text("copilot_enabled",      "true",    "Enable Copilot Assignment (true/false)")
dbutils.widgets.text("copilot_assignee",     "Copilot", "Copilot GitHub Username")
dbutils.widgets.text("genie_space_id",       "",        "Genie Space ID (leave blank to use Spark SQL fallback)")
dbutils.widgets.text("sql_warehouse_id",     "",        "SQL Warehouse ID for anomaly signal query")

SCORE_THRESHOLD      = float(dbutils.widgets.get("score_threshold") or "1.5")
DRY_RUN              = dbutils.widgets.get("dry_run").lower() == "true"
GITHUB_TOKEN         = dbutils.widgets.get("github_token").strip()
FALLBACK_REPO        = dbutils.widgets.get("github_fallback_repo").strip()
COPILOT_ENABLED      = dbutils.widgets.get("copilot_enabled").lower() != "false"
COPILOT_ASSIGNEE     = dbutils.widgets.get("copilot_assignee").strip() or "Copilot"
GENIE_SPACE_ID       = dbutils.widgets.get("genie_space_id").strip()

try:
    _repo_map_override = json.loads(dbutils.widgets.get("repo_map_json") or "{}")
except json.JSONDecodeError:
    _repo_map_override = {}

# Default repo map — override via repo_map_json parameter.
# Uses placeholder owner; set real values via the Workflow job parameters.
_DEFAULT_REPO_MAP = {
    "grizl-backend":       "<GITHUB_ORG>/grizl-backend",
    "grizl-log-forwarder": "<GITHUB_ORG>/grizl-backend",
    "grizl-frontend":      "<GITHUB_ORG>/grizl-frontend",
}
REPO_MAP = {**_DEFAULT_REPO_MAP, **_repo_map_override}

# Anomaly types safe for Copilot code remediation (same set as Fabric orchestrator).
SAFE_ACTIONABLE_TYPES = {
    "APPLICATION_ERROR",
    "FRONTEND_API_ERROR_SPIKE",
    "POST_DEPLOYMENT_ERROR",
    "HIGH_LATENCY",
}

print(f"Threshold: {SCORE_THRESHOLD} | DryRun: {DRY_RUN} | CopilotEnabled: {COPILOT_ENABLED}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Query the anomaly signal union view

# COMMAND ----------
def _sql_warehouse_query(sql_stmt, warehouse_id=None, timeout_s=60):
    """Run a SQL statement via the SQL warehouse (same path as sql-exec.sh).
    Returns list of dicts, one per row. Falls back to Spark on error."""
    host = spark.conf.get("spark.databricks.workspaceUrl")
    token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().apiToken().getOrElse(None)
    )
    if not token:
        raise RuntimeError("Could not obtain notebook API token")

    if not warehouse_id:
        warehouse_id = spark.conf.get("spark.databricks.sqlWarehouseId", "")

    url_base = f"https://{host}/api/2.0/sql/statements"
    body = json.dumps({
        "statement": sql_stmt,
        "warehouse_id": warehouse_id,
        "wait_timeout": f"{min(timeout_s, 50)}s",
        "on_wait_timeout": "CONTINUE",
    }).encode()
    req = urllib.request.Request(url_base, data=body, method="POST",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req) as resp:
        resp_data = json.loads(resp.read())

    stmt_id = resp_data["statement_id"]
    state   = resp_data.get("status", {}).get("state", "")

    deadline = _time_module.time() + timeout_s
    while state not in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED") and _time_module.time() < deadline:
        _time_module.sleep(3)
        poll_url = f"{url_base}/{stmt_id}"
        req2 = urllib.request.Request(poll_url,
            headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req2) as resp2:
            resp_data = json.loads(resp2.read())
        state = resp_data.get("status", {}).get("state", "")

    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL warehouse query failed: state={state}")

    result  = resp_data.get("result", {})
    manifest = resp_data.get("manifest", {})
    cols    = [c["name"] for c in (manifest.get("schema", {}).get("columns") or [])]
    rows    = result.get("data_array") or []
    return [dict(zip(cols, row)) for row in rows]


_WAREHOUSE_ID = dbutils.widgets.get("sql_warehouse_id").strip()
if not _WAREHOUSE_ID:
    try:
        _WAREHOUSE_ID = spark.conf.get("spark.databricks.sqlWarehouseId")
    except Exception:
        _WAREHOUSE_ID = ""

print(f"[INFO] Querying anomaly signals via SQL warehouse (id={_WAREHOUSE_ID or 'auto'})")

_ANOMALY_SQL = f"""
SELECT signal_name, alert_name, severity, anomaly_type, service, route,
       anomaly_score, baseline, actual, expected, total, ingest_timestamp,
       time_window_start, time_window_end, deployment_sha, dimensions, sql_view
FROM grizl.observability.grizl_recent_anomaly_signals
WHERE anomaly_score >= {SCORE_THRESHOLD}
ORDER BY anomaly_score DESC
LIMIT 50
"""

try:
    anomaly_rows = _sql_warehouse_query(_ANOMALY_SQL, warehouse_id=_WAREHOUSE_ID or None)
    print(f"[INFO] SQL warehouse query returned {len(anomaly_rows)} anomaly signal(s)")
except Exception as _wh_exc:
    print(f"[WARN] SQL warehouse query failed, falling back to Spark: {_wh_exc}")
    def _row_to_dict_basic(r):
        d = r.asDict()
        return {k: v.isoformat() if hasattr(v, "isoformat") else v for k, v in d.items()}
    anomaly_rows = [
        _row_to_dict_basic(row)
        for row in (
            spark.table("grizl.observability.grizl_recent_anomaly_signals")
            .filter(f"anomaly_score >= {SCORE_THRESHOLD}")
            .orderBy("anomaly_score", ascending=False)
            .limit(50)
        ).collect()
    ]

print(f"Active anomaly signals above threshold {SCORE_THRESHOLD}: {len(anomaly_rows)}")

if not anomaly_rows:
    dbutils.notebook.exit("no_anomalies")

def row_to_dict(row):
    d = row if isinstance(row, dict) else row.asDict()
    out = {}
    for k, v in d.items():
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif v is not None and not isinstance(v, (str, int, float, bool)):
            out[k] = str(v)
        else:
            out[k] = v
    return out

top = row_to_dict(anomaly_rows[0])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Collect evidence via Genie (AI/BI Data Agent) — falls back to Spark SQL
# MAGIC
# MAGIC Mirrors the Fabric Data Agent evidence step in `fabricKustoEvidence.service.js`.
# MAGIC When `genie_space_id` is set, Databricks Genie answers natural-language questions
# MAGIC over the `grizl.observability.*` Delta tables. Spark SQL is used as fallback.

# COMMAND ----------
import time as _time

def _safe_sql_str(value):
    if not value:
        return ""
    return str(value).replace("'", "\\'").replace(";", "")[:200]


def _genie_question(top_signal):
    """Build a natural-language evidence question from the anomaly signal."""
    service   = top_signal.get("service", "unknown")
    route     = top_signal.get("route") or ""
    error_sig = top_signal.get("error_signature") or ""
    atype     = top_signal.get("anomaly_type", "")
    signal    = top_signal.get("signal_name", "")
    score     = top_signal.get("anomaly_score", "")
    sha       = top_signal.get("deployment_sha") or ""

    scope_clause = ""
    if route:
        scope_clause += f" on route `{route}`"
    if error_sig:
        scope_clause += f" with error signature `{error_sig}`"
    if sha:
        scope_clause += f" for deployment `{sha}`"

    if atype in ("HIGH_LATENCY",):
        return (
            f"What is the p95 latency trend for service `{service}`{scope_clause} "
            f"over the last 30 minutes compared to the past 2 hours? "
            f"When did the latency spike start and what deployment SHA is running? "
            f"(Anomaly score: {score})"
        )
    elif atype in ("POST_DEPLOYMENT_ERROR",):
        return (
            f"What is the error rate for service `{service}`{scope_clause} "
            f"since the most recent deployment? How does it compare to before the deploy? "
            f"Show the top 5 error signatures. (Anomaly score: {score})"
        )
    else:
        return (
            f"Summarise the recent errors for service `{service}`{scope_clause} "
            f"in the last 30 minutes. What are the most frequent error signatures, "
            f"which routes are affected, and what is the error rate compared to the "
            f"2-hour baseline? Signal: {signal}. (Anomaly score: {score})"
        )


def _databricks_api(method, path, body=None, token=None, host=None):
    """Minimal urllib wrapper for Databricks REST API calls from within a notebook."""
    if host is None:
        host = spark.conf.get("spark.databricks.workspaceUrl")
    if token is None:
        token = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook().getContext().apiToken().getOrElse(None)
        )
    url = f"https://{host}{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(
        url, data=data, method=method.upper(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def collect_genie_evidence(top_signal, space_id, timeout_s=90):
    """
    Ask Databricks Genie (AI/BI Data Agent) a natural-language question about
    the anomaly and return its answer as evidence text.

    Mirrors fabricDataAgent.service.js — same NL→answer pattern, different runtime.
    Falls back to None on any error so the caller can use Spark SQL instead.
    """
    question = _genie_question(top_signal)
    print(f"[Genie] Asking: {question[:120]}...")
    try:
        # Start conversation
        conv = _databricks_api(
            "POST",
            f"/api/2.0/genie/spaces/{space_id}/start-conversation",
            {"content": question},
        )
        conv_id = conv.get("conversation_id")
        msg_id  = conv.get("message_id") or (conv.get("message") or {}).get("id")
        if not conv_id or not msg_id:
            print(f"[Genie] Unexpected start-conversation response: {conv}")
            return None

        # Poll until the message is complete
        deadline = _time.time() + timeout_s
        status   = None
        result   = None
        while _time.time() < deadline:
            msg = _databricks_api(
                "GET",
                f"/api/2.0/genie/spaces/{space_id}/conversations/{conv_id}/messages/{msg_id}",
            )
            status = (msg.get("status") or "").upper()
            if status in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
                result = msg
                break
            _time.sleep(3)

        if status != "COMPLETED":
            print(f"[Genie] Message ended with status {status!r} — skipping Genie evidence.")
            return None

        # Extract the text answer and any SQL/result the agent produced
        content_parts = result.get("attachments") or []
        answer_parts  = [result.get("content", "")]  # top-level NL answer
        for part in content_parts:
            if isinstance(part, dict):
                if part.get("text"):
                    answer_parts.append(part["text"].get("content", ""))
                if part.get("query"):
                    q = part["query"]
                    answer_parts.append(f"\n```sql\n{q.get('query', '')}\n```")
                    if q.get("description"):
                        answer_parts.append(q["description"])

        answer_text = "\n\n".join(p for p in answer_parts if p).strip()
        if not answer_text:
            print("[Genie] Empty answer received.")
            return None

        print(f"[Genie] Answer received ({len(answer_text)} chars).")
        return {
            "text": f"### Genie AI/BI Evidence\n\n**Question:** {question}\n\n**Answer:**\n\n{answer_text}",
            "source": "databricks_genie",
            "diagnostics": {
                "space_id":        space_id,
                "conversation_id": conv_id,
                "message_id":      msg_id,
                "tables":          ["grizl.observability.*"],
            },
        }
    except Exception as exc:
        print(f"[Genie] Error: {exc} — falling back to Spark SQL.")
        return None


def collect_spark_sql_evidence(top_signal):
    """Spark SQL fallback — used when Genie is not configured or fails."""
    service    = _safe_sql_str(top_signal.get("service", ""))
    route      = _safe_sql_str(top_signal.get("route", ""))
    error_sig  = _safe_sql_str(top_signal.get("error_signature", ""))
    deploy_sha = _safe_sql_str(top_signal.get("deployment_sha", ""))
    error_type = _safe_sql_str(top_signal.get("error_type", ""))

    route_filter = f"AND route = '{route}'"                 if route      else ""
    sig_filter   = f"AND error_signature = '{error_sig}'"  if error_sig  else ""
    sha_filter   = f"AND deployment_sha = '{deploy_sha}'"  if deploy_sha else ""
    etype_filter = f"AND error_type = '{error_type}'"      if error_type else ""

    lines = []

    try:
        rows_df = spark.sql(f"""
            SELECT ingest_timestamp, service, severity, event_type, method, route,
                   status_code, duration_ms, deployment_sha,
                   error_type, error_message, error_signature, trace_id
            FROM grizl.observability.raw_logs
            WHERE ingest_timestamp >= current_timestamp() - INTERVAL 14 DAY
              AND service = '{service}'
              {route_filter} {sig_filter} {sha_filter} {etype_filter}
            ORDER BY ingest_timestamp DESC LIMIT 10
        """)
        rows = [row_to_dict(r) for r in rows_df.collect()]
        cols = ["ingest_timestamp","service","severity","route",
                "status_code","duration_ms","deployment_sha","error_type","error_signature"]
        if rows:
            def mv(v):
                return "N/A" if v is None else str(v).replace("|","\\|")[:100]
            lines += [
                f"### Recent log rows — `{service}`",
                "", "| " + " | ".join(cols) + " |",
                "| " + " | ".join("---" for _ in cols) + " |",
                *["| " + " | ".join(mv(r.get(c)) for c in cols) + " |" for r in rows],
            ]
        else:
            lines.append(f"_No matching rows for `{service}` in the last 14 days._")
    except Exception as e:
        lines.append(f"_raw_logs query failed: {e}_")

    try:
        rate_df = spark.sql(f"""
            SELECT COUNT(*) AS total,
                   SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS error_5xx,
                   ROUND(SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0),4) AS error_rate,
                   ROUND(AVG(duration_ms),1) AS avg_ms,
                   MAX(deployment_sha) AS latest_sha
            FROM grizl.observability.raw_logs
            WHERE ingest_timestamp >= current_timestamp() - INTERVAL 1 HOUR
              AND service = '{service}'
        """)
        r = row_to_dict(rate_df.collect()[0])
        lines += [
            "", f"### Last-hour aggregate — `{service}`", "",
            f"- total: {r.get('total')}  |  5xx errors: {r.get('error_5xx')}  |  error_rate: {r.get('error_rate')}",
            f"- avg_ms: {r.get('avg_ms')}  |  latest_sha: {r.get('latest_sha')}",
        ]
    except Exception as e:
        lines.append(f"\n_Error rate query failed: {e}_")

    return {
        "text": "\n".join(lines) if lines else "_No evidence collected._",
        "source": "databricks_spark_sql",
        "diagnostics": {"service": service, "route": route, "tables": ["grizl.observability.raw_logs"]},
    }


def collect_evidence(top_signal, genie_space_id=""):
    """
    Genie-first evidence collection — mirrors the Fabric Data Agent pattern.
    Falls back to Spark SQL when Genie is not configured or unavailable.
    """
    if genie_space_id:
        genie_ev = collect_genie_evidence(top_signal, genie_space_id)
        if genie_ev:
            return genie_ev
        print("[Evidence] Genie unavailable — falling back to Spark SQL.")
    else:
        print("[Evidence] genie_space_id not set — using Spark SQL fallback.")
    return collect_spark_sql_evidence(top_signal)


evidence = collect_evidence(top, GENIE_SPACE_ID)
print(f"Evidence collected via {evidence['source']}: {len(evidence['text'])} chars")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Resolve target repository

# COMMAND ----------
def map_incident_to_repo(top_signal, repo_map, fallback_repo):
    service = top_signal.get("service", "")
    if service and repo_map.get(service):
        repo = repo_map[service]
        if not repo.startswith("<"):
            return repo
    if fallback_repo:
        return fallback_repo
    # Last resort: first non-placeholder repo in the map
    for repo in repo_map.values():
        if not repo.startswith("<"):
            return repo
    return None


target_repo = map_incident_to_repo(top, REPO_MAP, FALLBACK_REPO)
print(f"Target repo: {target_repo}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Classify remediation policy

# COMMAND ----------
def classify_policy(top_signal, evidence_text, target_repo):
    """Mirror of classifyPolicy() in fabricIncidentOrchestrator.service.js."""
    reasons = []

    anomaly_type   = (top_signal.get("anomaly_type") or "").upper()
    severity       = (top_signal.get("severity") or "").lower()
    route          = top_signal.get("route", "")
    error_sig      = top_signal.get("error_signature", "")
    deploy_sha     = top_signal.get("deployment_sha", "")

    has_scope        = bool(route or error_sig or deploy_sha)
    actionable_type  = anomaly_type in SAFE_ACTIONABLE_TYPES
    severity_rank    = {"critical": 3, "error": 2, "warning": 1, "info": 0}.get(severity, 0)
    code_hint_text   = " ".join(filter(None, [
        top_signal.get("alert_name", ""),
        top_signal.get("signal_name", ""),
        top_signal.get("error_type", ""),
        top_signal.get("error_signature", ""),
        evidence_text,
    ]))
    has_code_hint    = bool(re.search(
        r"route|file|stack|exception|deployment|regression|api|component",
        code_hint_text, re.IGNORECASE
    ))

    if not target_repo:
        reasons.append("No target repository could be resolved.")
    elif target_repo.startswith("<"):
        reasons.append(f"Target repository is a placeholder ({target_repo}); set repo_map_json parameter.")
    if not actionable_type:
        reasons.append(f"Anomaly type {anomaly_type} is not in SAFE_ACTIONABLE_TYPES.")
    if not has_scope:
        reasons.append("Incident does not include a route, error signature, or deployment SHA.")
    if not has_code_hint:
        reasons.append("Evidence does not contain enough code-actionable signal.")
    if severity_rank < 2:
        reasons.append(f"Severity {severity!r} is below remediation threshold (need error or critical).")

    safe_for_copilot = len(reasons) == 0
    return {
        "safe_for_copilot": safe_for_copilot,
        "action": "copilot_candidate" if safe_for_copilot else "issue_only",
        "target_repo": target_repo,
        "reasons": (
            ["Signal is scoped, code-actionable, and mapped to a repository."]
            if safe_for_copilot else reasons
        ),
    }


policy = classify_policy(top, evidence["text"], target_repo)
print(f"Policy: action={policy['action']} safe_for_copilot={policy['safe_for_copilot']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Build GitHub issue

# COMMAND ----------
def md_value(v):
    if v is None or v == "":
        return "N/A"
    return str(v).replace("|", "\\|")


def build_issue_title(top_signal):
    scope = (
        top_signal.get("route")
        or top_signal.get("error_signature")
        or top_signal.get("service")
        or "unknown scope"
    )
    return f"[Databricks Anomaly] {top_signal.get('alert_name', 'Anomaly Signal')} ({scope})"


def build_issue_body(top_signal, all_signals, evidence, policy):
    """Mirror of buildIssueBody() in fabricIncidentOrchestrator.service.js."""
    now = datetime.now(timezone.utc).isoformat()
    meta_rows = [
        ("Alert",           top_signal.get("alert_name")),
        ("Severity",        top_signal.get("severity")),
        ("Anomaly Type",    top_signal.get("anomaly_type")),
        ("Signal Name",     top_signal.get("signal_name")),
        ("Service",         top_signal.get("service")),
        ("Route",           top_signal.get("route")),
        ("Error Type",      top_signal.get("error_type")),
        ("Error Signature", top_signal.get("error_signature")),
        ("Deployment SHA",  top_signal.get("deployment_sha")),
        ("Anomaly Score",   top_signal.get("anomaly_score")),
        ("Baseline",        top_signal.get("baseline")),
        ("Actual",          top_signal.get("actual")),
        ("Time Window",     f"{top_signal.get('time_window_start','?')} → {top_signal.get('time_window_end','?')}"),
        ("SQL View",        top_signal.get("sql_view")),
        ("Detected At",     top_signal.get("detectedAt") or now),
    ]

    # Top 10 signals table
    signal_rows = [row_to_dict(r) for r in all_signals[:10]]
    signal_md_cols = ["signal_name", "service", "route", "anomaly_score", "actual", "baseline", "severity"]
    signals_table = (
        "| " + " | ".join(signal_md_cols) + " |\n"
        + "| " + " | ".join("---" for _ in signal_md_cols) + " |\n"
        + "\n".join(
            "| " + " | ".join(md_value(r.get(c)) for c in signal_md_cols) + " |"
            for r in signal_rows
        )
    )

    copilot_status = (
        "pending — will assign after issue creation" if policy["safe_for_copilot"] and COPILOT_ENABLED
        else ("skipped — policy did not mark this incident safe/scoped/code-actionable"
              if not policy["safe_for_copilot"]
              else "disabled — copilot_enabled=false")
    )

    lines = [
        "## Databricks Infrastructure Anomaly",
        "",
        "| Field | Value |",
        "|---|---|",
        *[f"| {k} | {md_value(v)} |" for k, v in meta_rows],
        "",
        "## Active Anomaly Signals",
        "",
        f"Total signals above threshold ({SCORE_THRESHOLD}): **{len(all_signals)}**",
        "",
        signals_table,
        "",
        "## Direct SQL Evidence",
        "",
        evidence["text"][:6000] if evidence["text"] else "_Evidence unavailable._",
        "",
        f"_Evidence source: `{evidence['source']}` — tables: {', '.join(evidence['diagnostics'].get('tables', []))}_",
        "",
        "## Remediation Policy Decision",
        "",
        f"**Action:** `{policy['action']}`",
        f"**Target repo:** `{policy['target_repo'] or 'unresolved'}`",
        f"**Safe for Copilot:** {'yes' if policy['safe_for_copilot'] else 'no'}",
        "",
        "**Rationale:**",
        *[f"- {r}" for r in policy["reasons"]],
        "",
        "## Copilot Coding Agent Handoff",
        "",
        f"**Status:** `{copilot_status}`",
        "",
        "## Validation Checklist",
        "",
        "- [ ] Confirm the Databricks anomaly signal is still firing or has recovered.",
        "- [ ] Check Cloud Run logs for the affected service and deployment SHA.",
        "- [ ] Run the SQL validation query from the evidence section above.",
        "- [ ] If code changes are made, verify the route smoke test and confirm raw_logs error rate returns to baseline.",
        "",
        "---",
        "_Auto-generated by the GRIZL Databricks Anomaly Signals Workflow._",
        f"_Data source: `grizl.observability.grizl_recent_anomaly_signals` — workspace: Databricks_",
    ]
    return "\n".join(lines)


issue_title = build_issue_title(top)
issue_body  = build_issue_body(top, anomaly_rows, evidence, policy)
issue_labels = ["databricks-anomaly", "observability", (top.get("severity") or "error").lower()]

print("Issue title:", issue_title)

if DRY_RUN:
    print("\n[DRY RUN] Issue body preview (first 500 chars):")
    print(issue_body[:500])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Create GitHub issue and assign Copilot

# COMMAND ----------
# GitHub Copilot is a GitHub App, not a regular user.
# REST assignees API does not accept App logins — must use GraphQL addAssigneesToAssignable.
# Node ID is stable: BOT_kgDOC9w8XQ (mirrors githubIssue.service.js DEFAULT_COPILOT_ASSIGNEE_NODE_ID)
COPILOT_BOT_NODE_ID = "BOT_kgDOC9w8XQ"


def github_rest(path, body_dict, token, method="POST"):
    """Call the GitHub REST API. Returns parsed JSON."""
    url        = f"https://api.github.com{path}"
    body_bytes = json.dumps(body_dict).encode("utf-8")
    req = urllib.request.Request(
        url, data=body_bytes, method=method,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "Content-Type":         "application/json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Length":       str(len(body_bytes)),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8") if e.fp else ""
        raise RuntimeError(f"GitHub REST {method} {path} → HTTP {e.code}: {err_body[:400]}") from e


def github_assign_copilot(token, issue_node_id):
    """
    Assign the Copilot GitHub App to an issue via GraphQL.
    Mirrors assignCopilotIssueWithGraphql() in githubIssue.service.js.
    Regular REST PATCH /assignees does not accept App logins.
    """
    query = """
    mutation AddCopilotAssignee($assignableId: ID!, $assigneeIds: [ID!]!) {
      addAssigneesToAssignable(input: { assignableId: $assignableId, assigneeIds: $assigneeIds }) {
        assignable {
          ... on Issue {
            number
            url
            assignees(first: 5) { nodes { login } }
          }
        }
      }
    }
    """
    body_bytes = json.dumps({
        "query":     query,
        "variables": {"assignableId": issue_node_id, "assigneeIds": [COPILOT_BOT_NODE_ID]},
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.github.com/graphql",
        data=body_bytes, method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Content-Length": str(len(body_bytes)),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8") if e.fp else ""
        raise RuntimeError(f"GitHub GraphQL → HTTP {e.code}: {err_body[:400]}") from e

    if data.get("errors"):
        raise RuntimeError(f"GitHub GraphQL errors: {data['errors']}")

    assignable = data.get("data", {}).get("addAssigneesToAssignable", {}).get("assignable", {})
    assigned = [n["login"] for n in assignable.get("assignees", {}).get("nodes", [])]
    if not any(l.lower() in ("copilot", "github-copilot") for l in assigned):
        raise RuntimeError(
            f"Copilot assignment did not persist. Assigned logins: {assigned}. "
            "Ensure Copilot Coding Agent is enabled for the repository."
        )
    return assignable


issue_result   = None
copilot_action = {"status": "skipped", "reason": "Not attempted."}

if DRY_RUN:
    print("[DRY RUN] Would create GitHub issue in:", target_repo)
    print("[DRY RUN] Labels:", issue_labels)
    copilot_action = {"status": "dry_run"}
elif not GITHUB_TOKEN:
    print("WARNING: github_token parameter is empty — skipping GitHub issue creation.")
    copilot_action = {"status": "skipped", "reason": "github_token not configured."}
elif not target_repo or target_repo.startswith("<"):
    print(f"WARNING: target_repo={target_repo!r} is unresolved — skipping issue creation.")
    copilot_action = {"status": "skipped", "reason": f"Unresolved target_repo: {target_repo}"}
else:
    # Create the GitHub issue
    issue_result = github_rest(
        f"/repos/{target_repo}/issues",
        {"title": issue_title, "body": issue_body, "labels": issue_labels},
        GITHUB_TOKEN,
    )
    issue_url     = issue_result.get("html_url", "")
    issue_number  = issue_result.get("number")
    issue_node_id = issue_result.get("node_id", "")
    print(f"GitHub issue created: #{issue_number} — {issue_url}")

    # Copilot handoff via GraphQL — Copilot is a GitHub App, not a regular user.
    # REST PATCH /assignees does not work for App logins; must use addAssigneesToAssignable.
    if policy["safe_for_copilot"] and COPILOT_ENABLED:
        try:
            github_assign_copilot(GITHUB_TOKEN, issue_node_id)
            copilot_action = {
                "status":   "assigned",
                "assignee": COPILOT_ASSIGNEE,
                "reason":   (
                    f"Assigned issue #{issue_number} to {COPILOT_ASSIGNEE} via GraphQL. "
                    "GitHub Copilot Coding Agent will create a pull request from the issue "
                    "when enabled for the repository."
                ),
                "url": issue_url,
            }
            print(f"Copilot assigned: {COPILOT_ASSIGNEE} → issue #{issue_number}")
        except Exception as e:
            copilot_action = {
                "status": "assignment_failed",
                "reason": str(e),
            }
            print(f"WARNING: Copilot assignment failed: {e}")
    else:
        reason = (
            "Policy did not mark this incident safe/scoped/code-actionable."
            if not policy["safe_for_copilot"]
            else "copilot_enabled=false"
        )
        copilot_action = {"status": "skipped", "reason": reason}

print("Copilot action:", copilot_action)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Log metrics to MLflow

# COMMAND ----------
if MLFLOW_AVAILABLE:
    with mlflow.start_run(run_name=f"anomaly-signals-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M')}"):
        mlflow.log_params({
            "score_threshold":   SCORE_THRESHOLD,
            "n_signals":         len(anomaly_rows),
            "top_signal":        top.get("signal_name", ""),
            "top_service":       top.get("service", ""),
            "top_anomaly_type":  top.get("anomaly_type", ""),
            "dry_run":           DRY_RUN,
            "target_repo":       target_repo or "",
            "policy_action":     policy["action"],
            "safe_for_copilot":  policy["safe_for_copilot"],
            "copilot_status":    copilot_action.get("status", ""),
        })
        mlflow.log_metrics({
            "anomaly_count":     len(anomaly_rows),
            "max_anomaly_score": float(top.get("anomaly_score") or 0),
            "critical_count":    sum(
                1 for r in anomaly_rows
                if row_to_dict(r).get("severity", "").lower() in ("critical", "error")
            ),
            "issue_created":     1 if issue_result else 0,
            "copilot_assigned":  1 if copilot_action.get("status") == "assigned" else 0,
        })
        if issue_result:
            mlflow.log_param("github_issue_url", issue_result.get("html_url", ""))
            mlflow.log_param("github_issue_number", str(issue_result.get("number", "")))
else:
    print(f"Metrics: anomaly_count={len(anomaly_rows)}, max_score={float(top.get('anomaly_score') or 0):.2f}, issue_created={1 if issue_result else 0}")

print("Done.")
dbutils.notebook.exit(json.dumps({
    "anomaly_count":   len(anomaly_rows),
    "issue_created":   bool(issue_result),
    "issue_url":       issue_result.get("html_url", "") if issue_result else "",
    "copilot_status":  copilot_action.get("status", ""),
    "policy_action":   policy["action"],
    "dry_run":         DRY_RUN,
}))
