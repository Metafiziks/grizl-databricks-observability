# Databricks Incident Orchestrator

The Databricks Incident Orchestrator is the backend action layer between
Databricks anomaly signals and the GRIZL GitHub/Copilot remediation flow.
It mirrors the [Fabric Incident Orchestrator](../../grizl-fabric-observability/docs/fabric-incident-orchestrator.md)
but replaces the Fabric Activator/Data Agent layer with Databricks Workflows and Genie.

```text
Databricks Workflow (anomalySignalsJob, 5-min schedule)
  OR Databricks SQL Alert (threshold trigger)
  -> POST /api/databricks/incidents
  -> Genie (AI/BI) evidence query over grizl.observability.*
  -> GitHub issue in the mapped repository
  -> policy-gated Copilot Coding Agent issue assignment
```

The separation of concerns is intentional:

| Layer | Responsibility |
|---|---|
| Databricks Workflow job | Run anomaly signal union, format rich payload, POST to webhook |
| Databricks SQL Alert | Threshold-based trigger for simple conditions (backup/complement) |
| Genie (AI/BI) | Read-only natural language evidence over Delta tables |
| Backend orchestrator | Payload validation, policy, repo mapping, issue creation, Copilot handoff |
| Copilot Coding Agent | Code remediation only after policy says incident is safe/scoped/code-actionable |

## Webhook endpoint

Configure the Databricks Workflow job parameter and SQL Alert webhook destination to call:

```text
POST https://<backend-host>/api/databricks/incidents
```

Authentication header (set as a notebook parameter in the Workflow job, and as a
custom header in the SQL Alert webhook destination):

```http
x-grizl-databricks-secret: <DATABRICKS_ALERT_WEBHOOK_SECRET>
```

The route rejects requests when `DATABRICKS_ALERT_WEBHOOK_SECRET` is missing, the shared
secret is wrong, JSON is invalid, or the payload exceeds `DATABRICKS_ALERT_MAX_PAYLOAD_BYTES`
(default 64 KB).

## Webhook payload format

The Workflow job (`notebooks/04_anomaly_signals_workflow.py`) sends a payload that mirrors
the Fabric Activator webhook shape, extended with Databricks-specific fields:

```json
{
  "alertName":       "Backend HTTP Error Rate Anomaly",
  "severity":        "ERROR",
  "service":         "grizl-backend",
  "route":           "/api/memes",
  "deploymentSha":   "abc1234",
  "errorType":       null,
  "errorSignature":  null,
  "signalName":      "backend_http_error_rate",
  "anomalyScore":    2.91,
  "baseline":        0.015,
  "actual":          0.24,
  "expected":        0.015,
  "anomalyType":     "APPLICATION_ERROR",
  "timeWindowStart": "2026-07-20T19:00:00+00:00",
  "timeWindowEnd":   "2026-07-20T19:05:00+00:00",
  "dimensions":      {"service": "grizl-backend", "route": "/api/memes"},
  "sqlView":         "BackendHttpErrorRateAnomalies",
  "sqlQueryRef":     "sql/grizl-anomaly-signals.sql#BackendHttpErrorRateAnomalies",
  "allSignals":      [...],
  "source":          "databricks-workflow",
  "detectedAt":      "2026-07-20T19:05:03.124Z"
}
```

The `allSignals` array contains up to 50 anomaly rows from `grizl_recent_anomaly_signals`,
ordered by descending `anomaly_score`.

## Environment variables

```dotenv
DATABRICKS_INCIDENT_ORCHESTRATOR_ENABLED=true
DATABRICKS_ALERT_WEBHOOK_SECRET=<shared-webhook-secret>
DATABRICKS_ALERT_MAX_PAYLOAD_BYTES=65536

DATABRICKS_HOST=https://<YOUR_WORKSPACE>.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=<sql-warehouse-id>
DATABRICKS_CATALOG=grizl
DATABRICKS_SCHEMA=observability
DATABRICKS_RAW_TABLE=raw_logs

# Genie (AI/BI) for incident evidence queries
DATABRICKS_GENIE_SPACE_ID=<genie-space-id>

# Service principal OAuth M2M
# Used at runtime to mint workspace tokens for Genie and SQL fallback calls.
DATABRICKS_CLIENT_ID=<service-principal-client-id>
DATABRICKS_CLIENT_SECRET=<service-principal-secret>  # GCP Secret Manager

GITHUB_TOKEN=<github-token-with-issues-write>          # GCP Secret Manager
GITHUB_REPO=<github-owner>/<backend-repo>
DATABRICKS_INCIDENT_REPO_MAP_JSON={"grizl-backend":"<owner>/<backend-repo>","grizl-frontend":"<owner>/<frontend-repo>","grizl-log-forwarder":"<owner>/<backend-repo>"}

# Copilot handoff (same as Fabric version)
COPILOT_CODING_AGENT_ASSIGNMENT_ENABLED=true
COPILOT_CODING_AGENT_ASSIGNEE=Copilot
COPILOT_CODING_AGENT_ASSIGNEE_NODE_ID=<node-id>
COPILOT_CODING_AGENT_WEBHOOK_URL=
```

Production auth uses an OAuth M2M service principal. Grant the SP:
`USE CATALOG grizl`, `USE SCHEMA observability`, `SELECT` on all views and tables.
`DATABRICKS_CLIENT_SECRET` stays in GCP Secret Manager — not in repository variables.

## GitHub Actions / Cloud Run deployment

Set these GitHub repository variables before enabling production alerts:

```text
DATABRICKS_INCIDENT_ORCHESTRATOR_ENABLED=true
DATABRICKS_ALERT_MAX_PAYLOAD_BYTES=65536
DATABRICKS_HOST=https://<YOUR_WORKSPACE>.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=<warehouse-id>
DATABRICKS_CATALOG=grizl
DATABRICKS_SCHEMA=observability
DATABRICKS_RAW_TABLE=raw_logs
DATABRICKS_GENIE_SPACE_ID=<genie-space-id>
DATABRICKS_CLIENT_ID=<service-principal-client-id>
DATABRICKS_INCIDENT_REPO_MAP_JSON={"grizl-backend":"<owner>/<backend-repo>","grizl-frontend":"<owner>/<frontend-repo>","grizl-log-forwarder":"<owner>/<backend-repo>"}
COPILOT_CODING_AGENT_ASSIGNMENT_ENABLED=true
COPILOT_CODING_AGENT_ASSIGNEE=Copilot
COPILOT_CODING_AGENT_ASSIGNEE_NODE_ID=<node-id>
```

Create or rotate these GCP Secret Manager secrets in the deploy project:

```text
DATABRICKS_ALERT_WEBHOOK_SECRET
DATABRICKS_CLIENT_SECRET
GITHUB_TOKEN
```

## Genie evidence call

The orchestrator queries Genie with an incident-specific prompt to gather evidence
before creating the GitHub issue:

```text
POST https://{host}/api/2.0/genie/spaces/{space_id}/start-conversation
Authorization: Bearer <workspace-token>
Content-Type: application/json

{"content": "Investigate the incident: Backend HTTP error spike on grizl-backend route /api/memes. Show me: (1) recent errors with error type and message, (2) the error rate over the last hour compared to the baseline, (3) which deployment SHA is currently running, and (4) any correlated anomaly signals."}
```

Response includes `conversation_id` and `message_id`. The orchestrator polls:

```text
GET https://{host}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}
```

Until `status` = `COMPLETED` (or `FAILED`), with `attachments` containing the Genie answer.

Genie operates over the tables configured in the space (`grizl.observability.*`).
It does not create issues or perform remediation.

## Direct Databricks SQL fallback

If the Genie call is unavailable or returns an error, the orchestrator falls back to
a direct SQL query against `raw_logs` using the Databricks SQL Statements API:

```text
POST https://{host}/api/2.0/sql/statements
Authorization: Bearer <workspace-token>
Content-Type: application/json

{
  "warehouse_id": "<warehouse-id>",
  "catalog": "grizl",
  "schema": "observability",
  "statement": "SELECT ingest_timestamp, service, error_type, error_message, error_signature, route, deployment_sha FROM application_errors WHERE service = 'grizl-backend' AND ingest_timestamp >= current_timestamp() - INTERVAL 1 HOUR ORDER BY ingest_timestamp DESC LIMIT 20",
  "wait_timeout": "30s"
}
```

The fallback evidence is embedded in the GitHub issue under `Direct SQL Evidence Fallback`.

## SQL anomaly-signal layer

The production anomaly detection layer uses z-score views over `raw_logs` and is
defined in [`../sql/grizl-anomaly-signals.sql`](../sql/grizl-anomaly-signals.sql).
Apply after the base views exist:

```bash
npm --prefix databricks run sql:anomaly-signals:dry-run
npm --prefix databricks run sql:anomaly-signals
```

High-value anomaly views:

| View | Purpose |
|---|---|
| `backend_http_error_rate_anomalies` | service/route 5xx error-rate anomalies (z-score) |
| `route_latency_anomalies` | route p95 latency anomalies when `duration_ms` is populated |
| `error_signature_spike_anomalies` | repeated error-signature spikes |
| `forwarder_freshness_drop_anomalies` | forwarder freshness/drop anomalies |
| `forwarder_drop_failure_anomalies` | forwarder skipped/retry/nack/failure spikes |
| `post_deployment_regression_anomalies` | post-deploy service error-rate regressions by `deployment_sha` |
| `grizl_recent_anomaly_signals` | union view for SQL Alert and Workflow trigger |

The Databricks Workflow job (`notebooks/04_anomaly_signals_workflow.py`) queries
`grizl_recent_anomaly_signals` on a 5-minute schedule and POSTs to the backend
when any row exceeds the `score_threshold` (default: 1.5).

## Lakehouse Monitoring complement

Lakehouse Monitoring generates managed drift/quality profiles in
`grizl.observability_monitors.*`. SQL Alerts can be built directly on the drift
metrics table (see `sql/grizl-alert-queries.sql` Alert 8) for a managed,
schedule-based drift detection layer that runs alongside the z-score views.

## Synthetic webhook smoke payload

```bash
curl -X POST "https://<backend-host>/api/databricks/incidents" \
  -H "Content-Type: application/json" \
  -H "x-grizl-databricks-secret: <DATABRICKS_ALERT_WEBHOOK_SECRET>" \
  -d '{
    "alertName": "Backend HTTP Error Spike",
    "severity": "ERROR",
    "service": "grizl-backend",
    "route": "/api/memes",
    "deploymentSha": "abc1234",
    "errorType": "MongoTimeoutError",
    "errorSignature": "MongoTimeoutError:/api/memes",
    "signalName": "backend_http_error_rate",
    "anomalyScore": 2.91,
    "baseline": 0.015,
    "actual": 0.24,
    "expected": 0.015,
    "anomalyType": "APPLICATION_ERROR",
    "sqlView": "BackendHttpErrorRateAnomalies",
    "sqlQueryRef": "sql/grizl-anomaly-signals.sql#BackendHttpErrorRateAnomalies",
    "dimensions": {
      "service": "grizl-backend",
      "route": "/api/memes"
    },
    "timeWindowStart": "2026-07-20T19:00:00Z",
    "timeWindowEnd":   "2026-07-20T19:05:00Z",
    "source": "databricks-workflow",
    "detectedAt": "2026-07-20T19:05:03Z"
  }'
```

Expected result: a `202 Accepted` response with the normalized incident summary,
created GitHub issue, policy decision, and `copilotAction`.

## Policy and Copilot handoff

Identical to the Fabric version. The orchestrator creates a GitHub issue for every
accepted Databricks incident. It only considers Copilot handoff when all of these are true:

1. The anomaly type is code-actionable (`APPLICATION_ERROR`, `FRONTEND_API_ERROR_SPIKE`, `HIGH_LATENCY`, or `POST_DEPLOYMENT_ERROR`).
2. The incident has scope (`route`, `page`, `errorSignature`, or `deploymentSha`).
3. The service maps to a repository via `DATABRICKS_INCIDENT_REPO_MAP_JSON`.
4. Genie evidence (or SQL fallback) contains code-actionable signal.
5. Active remediation guardrails do not conflict.

## Known limitations

- Genie space creation and table configuration is UI-only; there is no Databricks API
  for creating or fully configuring Genie spaces programmatically.
- The z-score anomaly views scan 2 days of data on every evaluation. For high-volume
  tables, add ZORDER on `(service, event_type, severity)` and consider materializing
  anomaly scores via a Workflow job that writes to `grizl.observability.ml_anomaly_scores`.
- The Databricks SQL Alert webhook payload contains only alert metadata. For full
  anomaly context (score, baseline, actual, dimensions), use the Workflow job
  (`notebooks/04_anomaly_signals_workflow.py`), which is the recommended alerting path.
- The Workflow job requires a cluster; use the Asset Bundle single-node config
  (`databricks/databricks.yml`) for minimal cost.
