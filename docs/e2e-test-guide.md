# GRIZL Databricks Observability — End-to-End Test Guide

Two test paths:

| Path | What it covers | Time |
|---|---|---|
| **A — Fast (synthetic data)** | Anomaly detection → GitHub issue → Copilot | ~30 min |
| **B — Full pipeline** | GCS ingest → DLT → anomaly detection → GitHub issue | ~2 hr |

Run Path A first to verify the action layer, then Path B to verify the full ingestion pipeline.

---

## Prerequisites

### 1. Install Databricks CLI

```bash
brew tap databricks/tap
brew install databricks
databricks --version
```

### 2. Authenticate

```bash
databricks auth login --host https://<YOUR_WORKSPACE>.cloud.databricks.com
# Follow browser prompt to complete OAuth
databricks auth token  # should print a Bearer token
```

### 3. Create the env file

```bash
cd databricks
cp config/grizl.databricks.env.example config/grizl.databricks.env
# Edit grizl.databricks.env — fill in at minimum:
#   DATABRICKS_HOST, DATABRICKS_WAREHOUSE_ID,
#   GCP_PROJECT_ID, PUBSUB_TOPIC
```

### 4. Provision Unity Catalog resources

If not already done:

```bash
npm --prefix databricks run preflight   # verify CLI, auth, warehouse
npm --prefix databricks run provision   # CREATE CATALOG grizl, schemas, volume
npm --prefix databricks run sql:observability      # CREATE TABLE raw_logs + views
npm --prefix databricks run sql:anomaly-signals    # CREATE anomaly signal views
```

---

## Path A — Fast: Synthetic Data

### A1. Inject synthetic log rows

Open the Databricks SQL editor (or run via `databricks sql execute`) and paste:

```sql
-- Inject a realistic error spike for grizl-backend /api/memes
INSERT INTO grizl.observability.raw_logs
SELECT
  uuid()                                AS log_id,
  TIMESTAMP_SECONDS(UNIX_TIMESTAMP(current_timestamp()) - (pos * 30)) AS ingest_timestamp,
  'grizl-backend'                       AS service,
  'production'                          AS environment,
  'ERROR'                               AS severity,
  'http_request'                        AS event_type,
  'POST'                                AS method,
  '/api/memes'                          AS route,
  500                                   AS status_code,
  1800 + (pos * 50)                     AS duration_ms,
  'MongoTimeoutError'                   AS error_type,
  CONCAT('Connection timed out after ', pos * 100, 'ms') AS error_message,
  'MongoTimeoutError:/api/memes'        AS error_signature,
  'abc1234'                             AS deployment_sha,
  uuid()                                AS trace_id,
  uuid()                                AS request_id,
  CAST(NULL AS STRING)                  AS page,
  CAST(NULL AS STRING)                  AS component,
  CAST(NULL AS STRING)                  AS user_id,
  NULL                                  AS api_status,
  current_timestamp()                   AS log_date,
  current_timestamp()                   AS source_timestamp
FROM (SELECT explode(sequence(1, 40)) AS pos);

-- Inject 10 baseline rows from 3 days ago so the z-score has a denominator
INSERT INTO grizl.observability.raw_logs
SELECT
  uuid(), TIMESTAMP_SECONDS(UNIX_TIMESTAMP(current_timestamp()) - 259200 - (pos * 300)),
  'grizl-backend', 'production', 'INFO', 'http_request', 'POST', '/api/memes',
  200, 200, NULL, NULL, NULL, 'abc1233', uuid(), uuid(),
  NULL, NULL, NULL, NULL, current_timestamp(), current_timestamp()
FROM (SELECT explode(sequence(1, 10)) AS pos);
```

### A2. Verify anomaly signals fire

```sql
SELECT * FROM grizl.observability.grizl_recent_anomaly_signals
ORDER BY anomaly_score DESC
LIMIT 5;
```

Expected: at least one row for `grizl-backend` with `anomaly_score > 1.5`.

If no rows appear, check that the baseline window (2-day lookback excluding last 15 min) has enough rows. The synthetic INSERT above seeds both windows.

### A3. Run the Workflow notebook — dry run

In the Databricks UI:
- Open `notebooks/04_anomaly_signals_workflow`
- Set widget `dry_run = true`
- Set `score_threshold = 1.5`
- Leave `github_token` blank
- **Run All**

Expected output:
```
Active anomaly signals above threshold 1.5: 1
Evidence collected: <N> chars
Target repo: <from repo_map_json or placeholder>
Policy: action=copilot_candidate safe_for_copilot=True   (or issue_only if scope missing)
[DRY RUN] Issue body preview:
## Databricks Infrastructure Anomaly
...
```

### A4. Run the Workflow notebook — live GitHub issue

Set widget values:

| Widget | Value |
|---|---|
| `dry_run` | `false` |
| `github_token` | your GitHub PAT (`issues:write`, `pull_requests:write`) |
| `repo_map_json` | `{"grizl-backend":"<owner>/grizl-backend","grizl-frontend":"<owner>/grizl-frontend","grizl-log-forwarder":"<owner>/grizl-backend"}` |
| `github_fallback_repo` | `<owner>/grizl-backend` |
| `copilot_enabled` | `true` |
| `copilot_assignee` | `Copilot` |

**Run All**

Expected:
- `GitHub issue created: #<N> — https://github.com/...`
- `Copilot assigned: Copilot → issue #<N>`  (requires Copilot Coding Agent enabled on the repo)
- Issue appears in GitHub with labels `databricks-anomaly`, `observability`, `error`
- Issue body contains: metadata table, recent log rows, error rate, policy decision, validation checklist
- MLflow run logged at `Experiments → anomaly-signals-<timestamp>`

If Copilot assignment fails with "did not persist", Copilot Coding Agent is not enabled for that repository. The issue is still created; enable it in repo Settings → Copilot → Coding agent.

### A5. Verify MLflow run

In Databricks:
- Open **Experiments** → search `anomaly-signals`
- Confirm `anomaly_count`, `max_anomaly_score`, `issue_created=1`, `copilot_assigned=1`

### A6. Clean up synthetic data (optional)

```sql
DELETE FROM grizl.observability.raw_logs
WHERE deployment_sha IN ('abc1234', 'abc1233')
  AND error_signature = 'MongoTimeoutError:/api/memes';
```

---

## Path B — Full Pipeline: Real Telemetry

### B1. Create the GCS bucket and export subscription

```bash
source databricks/config/grizl.databricks.env
GCS_BUCKET=grizl-logs-databricks-export   # pick a name
npm --prefix databricks run gcs-subscription:dry-run  # preview
GCS_BUCKET=$GCS_BUCKET npm --prefix databricks run gcs-subscription
```

Or directly:
```bash
GCS_BUCKET=grizl-logs-databricks-export \
GCP_PROJECT_ID=<your-project> \
PUBSUB_TOPIC=<your-topic> \
bash databricks/scripts/gcs-subscription.sh grizl-logs-databricks-export
```

Grant Databricks access to the bucket:
```bash
gsutil iam ch \
  serviceAccount:<DATABRICKS_GSA>@<GCP_PROJECT_ID>.iam.gserviceaccount.com:objectViewer \
  gs://$GCS_BUCKET
```

### B2. Update the bundle with the GCS path

```bash
databricks bundle deploy --target dev \
  --var gcs_logs_path=gs://grizl-logs-databricks-export/logs/ \
  --var github_token=<token> \
  --var repo_map_json='{"grizl-backend":"<owner>/grizl-backend","grizl-frontend":"<owner>/grizl-frontend","grizl-log-forwarder":"<owner>/grizl-backend"}'
```

### B3. Start the DLT pipeline

In Databricks UI:
**Workflows → Delta Live Tables → grizl-autoloader-pipeline → Start**

Or via CLI:
```bash
databricks pipelines start --pipeline-id <DATABRICKS_DLT_PIPELINE_ID>
```

### B4. Trigger real telemetry

Send a frontend error to grizl-backend's telemetry sink:
```bash
curl -X POST "https://grizl-backend-<hash>-uc.a.run.app/api/telemetry" \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "client_error",
    "severity": "ERROR",
    "errorType": "TestError",
    "errorMessage": "Databricks E2E test synthetic error",
    "errorSignature": "TestError:/test-route",
    "page": "/test-route",
    "deploymentSha": "e2e-test-sha"
  }'
# Expected: {"accepted":true}
```

Send 30+ identical requests to generate enough volume for anomaly detection:
```bash
for i in $(seq 1 35); do
  curl -s -X POST "https://grizl-backend-<hash>-uc.a.run.app/api/telemetry" \
    -H "Content-Type: application/json" \
    -d '{"eventType":"client_error","severity":"ERROR","errorType":"TestError","errorSignature":"TestError:/test-route","page":"/test-route"}' \
    > /dev/null
done
```

### B5. Wait for GCS files

The Pub/Sub export subscription writes every 60 seconds. After ~2 minutes:
```bash
gsutil ls gs://grizl-logs-databricks-export/logs/
```

Expected: `.jsonl` files from the Cloud Logging sink.

### B6. Trigger the DLT pipeline update

```bash
databricks pipelines start --pipeline-id <DATABRICKS_DLT_PIPELINE_ID> --full-refresh
```

Or wait for the next scheduled run (continuous mode is off by default).

### B7. Verify the Delta table

```sql
SELECT ingest_timestamp, service, severity, event_type, error_type, error_signature
FROM grizl.observability.raw_logs
ORDER BY ingest_timestamp DESC
LIMIT 10;
```

### B8. Run the Workflow notebook (same as A4)

At this point the real rows are in `raw_logs`. Run step A2 (check anomaly signals) and A4 (create GitHub issue). The issue body will contain the real Cloud Run telemetry rows as evidence.

---

## Deploy the 5-minute Workflow in production

Once the test passes, unpause the Workflow in production:

```bash
databricks bundle deploy --target prod \
  --var gcs_logs_path=gs://<BUCKET>/logs/ \
  --var github_token=<prod-token> \
  --var repo_map_json='{"grizl-backend":"<owner>/grizl-backend","grizl-frontend":"<owner>/grizl-frontend","grizl-log-forwarder":"<owner>/grizl-backend"}'
```

The `prod` target sets `pause_status: UNPAUSED` (see `databricks/databricks.yml`).
The job will fire every 5 minutes, query `grizl_recent_anomaly_signals`, and create a GitHub
issue with Copilot assignment for any signal above the threshold.

---

## Smoke curl (webhook path — optional)

If you later add a `POST /api/databricks/incidents` endpoint to `grizl-backend`,
test it with the synthetic payload from `docs/databricks-incident-orchestrator.md`:

```bash
curl -X POST "https://grizl-backend-<hash>-uc.a.run.app/api/databricks/incidents" \
  -H "Content-Type: application/json" \
  -H "x-grizl-databricks-secret: <DATABRICKS_ALERT_WEBHOOK_SECRET>" \
  -d @- <<'EOF'
{
  "alertName": "Backend HTTP Error Spike",
  "severity": "ERROR",
  "service": "grizl-backend",
  "route": "/api/memes",
  "deploymentSha": "abc1234",
  "errorType": "MongoTimeoutError",
  "errorSignature": "MongoTimeoutError:/api/memes",
  "signalName": "backend_http_error_rate",
  "anomalyScore": 2.91,
  "source": "databricks-workflow"
}
EOF
```

---

## What to check if things go wrong

| Symptom | Check |
|---|---|
| No anomaly signals | Did the synthetic INSERT produce both error rows AND baseline rows? Run the INSERT again with a lower ratio. Check the z-score SQL view directly. |
| `target_repo` is a placeholder | Set `repo_map_json` widget with real `owner/repo` values. |
| Copilot assignment fails | Copilot Coding Agent must be enabled for the repository: Settings → Copilot → Coding agent. |
| DLT pipeline fails | Check pipeline logs for GCS auth errors. Ensure Databricks SA has `objectViewer` on the bucket. |
| GCS files not appearing | Verify the Cloud Storage export subscription is `ACTIVE`: `gcloud pubsub subscriptions describe grizl-logs-gcs-export`. |
| `raw_logs` table empty after DLT | Check Auto Loader schema inference volume path. Run `notebooks/01_autoloader_pipeline` manually in the workspace. |
