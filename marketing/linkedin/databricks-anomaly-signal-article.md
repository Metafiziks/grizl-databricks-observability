# Part VI: I Ported the Panic Button to Databricks and Made Delta Lake File the Incident Report

Subtitle: A tour through Pub/Sub → GCS → Auto Loader → Delta → SQL z-scores → Genie → GitHub issues, and the bug where Python code silently became a markdown poem for an entire workflow execution.

---

The Fabric version worked.

Which meant, obviously, I had to port it to Databricks.

Not because it was broken. Not because something was missing. But because I have made certain life choices, and one of them is that if I build an anomaly-detection-to-GitHub-issue pipeline on one cloud-adjacent data platform, I am going to build it on the other one too.

That is the energy. That is the posture. I regret nothing.

The public sanitized package is here:

https://github.com/Metafiziks/grizl-databricks-observability

## The premise: same snitch, different plumbing

Recap for people who did not read Part V:

The system watches application telemetry, detects anomalies, assembles forensic evidence, and creates GitHub issues with enough context for Copilot — or a human — to know what happened, what route was involved, what deployment SHA is suspicious, and what the baseline looked like before everything went wrong.

The Fabric version did this with Eventhouse, KQL, `series_decompose_anomalies()`, Fabric Activator, an external incident orchestrator webhook, and Fabric Data Agent for natural-language evidence retrieval.

The Databricks version does this with:

- Pub/Sub → Cloud Storage export subscription → Auto Loader → Delta Lake
- Unity Catalog SQL views instead of KQL functions
- z-score arithmetic in SQL instead of `series_decompose_anomalies()`
- Databricks Workflow instead of Fabric Activator
- Genie (AI/BI) instead of Fabric Data Agent
- The Workflow notebook calling GitHub directly, instead of routing through a webhook

Same outcome. Different nouns.

## The ingestion path: zero changes to existing services

This was the part I was most pleased about.

The existing GRIZL infrastructure already had:

1. Apps writing structured logs to Cloud Logging.
2. Cloud Logging routing to a Pub/Sub topic.
3. A pull subscription consuming those messages in `grizl-log-forwarder`.

Adding Databricks ingestion required one command:

```bash
gcloud pubsub subscriptions create grizl-logs-gcs-export \
  --topic=grizl-log-topic \
  --cloud-storage-bucket=<bucket> \
  --cloud-storage-file-prefix=logs/ \
  --cloud-storage-file-suffix=.jsonl \
  --cloud-storage-max-duration=60s \
  --cloud-storage-output-format=text
```

That is it. One Cloud Storage export subscription alongside the existing pull subscription. The forwarder keeps running. The apps keep running. Nobody noticed. Nothing broke.

The Databricks side picks up from there with a DLT Auto Loader pipeline using `cloudFiles`:

```python
spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "json") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .load(gcs_logs_path)
```

Auto Loader handles schema inference, schema evolution, and the incremental ingest bookkeeping so we do not have to.

The target: `grizl.observability.raw_logs`, a Delta table in Unity Catalog. Partitioned by `log_date`. All columns from the Pub/Sub envelope plus derived fields for `source_timestamp`, `error_signature`, and `log_date`.

## The anomaly detection: z-score in SQL, no model required

The Fabric version used `series_decompose_anomalies()`, an Eventhouse-native time-series decomposition function. Databricks Delta does not have that. 

What it does have is SQL. Specifically: `AVG`, `STDDEV`, `FLOOR`, `UNIX_TIMESTAMP`, and the ability to `CREATE VIEW` over those computations so they run fresh on every query.

The z-score anomaly detection pattern:

```sql
WITH time_series AS (
  SELECT
    TIMESTAMP_SECONDS(FLOOR(UNIX_TIMESTAMP(ingest_timestamp) / 300) * 300) AS time_bin,
    service, route,
    COUNT(*)                                                                AS requests,
    SUM(CASE WHEN status_code >= 500 THEN 1.0 ELSE 0.0 END) / COUNT(*)   AS error_rate
  FROM grizl.observability.http_requests
  WHERE ingest_timestamp >= current_timestamp() - INTERVAL 2 DAYS
  GROUP BY 1, 2, 3
),
baseline_stats AS (
  SELECT service, route,
    AVG(error_rate)    AS baseline_mean,
    STDDEV(error_rate) AS baseline_stddev
  FROM time_series
  WHERE time_bin < current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY 1, 2
)
SELECT ...
  (ts.error_rate - b.baseline_mean) / b.baseline_stddev AS anomaly_score
FROM time_series ts
JOIN baseline_stats b USING (service, route)
WHERE ts.time_bin >= current_timestamp() - INTERVAL 15 MINUTES
  AND ts.requests >= 20
  AND b.baseline_stddev > 0
  AND (ts.error_rate - b.baseline_mean) / b.baseline_stddev >= 1.5
```

`FLOOR(UNIX_TIMESTAMP(ts) / 300) * 300` buckets events into 5-minute bins without a time-series library.

The detection window is the last 15 minutes. The baseline window is everything older than 15 minutes in the last 2 days. Standard deviation needs at least two distinct values to be nonzero, which is the natural minimum-data requirement.

The full anomaly signal set:

| View | Signal |
|---|---|
| `backend_http_error_rate_anomalies` | backend 5xx/error-rate anomalies by service and route |
| `route_latency_anomalies` | route p95 latency anomalies |
| `error_signature_spike_anomalies` | repeated error-signature spikes |
| `forwarder_freshness_drop_anomalies` | forwarder healthy event volume drops |
| `forwarder_drop_failure_anomalies` | skipped/retry/nack/failure spikes |
| `post_deployment_regression_anomalies` | post-deployment regressions by deployment SHA |
| `grizl_recent_anomaly_signals` | UNION of all views — the Workflow trigger query |

`grizl_recent_anomaly_signals` is the single query the Workflow runs every five minutes. If it returns rows, an incident is in progress. If it does not, the notebook exits cleanly and Databricks does not care.

This is the same conceptual approach as `series_decompose_anomalies()` without requiring a time-series ML model. Just SQL. SQL that any engineer can read, modify, and understand at 2 AM when their phone is going off and they are trying to figure out why the baseline has variance.

## The bug I introduced that I would like to never speak of again

I need to tell you about the notebook.

Databricks Python notebooks are organized as cells, separated by `# COMMAND ----------` comments. Each cell is either Python or magic (`%md`, `%sql`, `%sh`). If a cell starts with `# MAGIC %md`, Databricks renders everything in that cell as markdown.

Everything.

The anomaly signals Workflow notebook was structured like this:

```python
# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Query the anomaly signal union view

def _sql_warehouse_query(sql_stmt, warehouse_id=None, timeout_s=60):
    ...

# (several hundred lines of Python follow)
```

The Python code after the `%md` header was inside the same cell.

Which means Databricks rendered it as a markdown code block.

Which means it never executed.

Which means for every single test run, the notebook:

1. Loaded imports (CMD1 — actual Python cell, ran correctly).
2. Rendered seven cells of Python as decorative documentation.
3. Exited in 15–26 seconds with `SUCCESS`.

No anomaly query. No evidence collection. No GitHub issue. Just a very fast, very confident notebook that had done essentially nothing and felt great about it.

The fix was to add `# COMMAND ----------` between each section header and its code block. Seven inserts. The notebook went from completing in 15 seconds to 88 seconds on the first real execution, which is how I knew it was actually working.

The GitHub issue appeared 54 seconds later.

I do not recommend this debugging experience but I will admit that "the Python became markdown" is a sentence I will be using at some point in the future to explain how software development works.

## Genie: natural-language evidence over Delta tables

The Fabric version used Fabric Data Agent — a natural-language-to-KQL interface over the Eventhouse database. The Databricks equivalent is Genie, which is a natural-language-to-SQL interface over Delta tables.

Creating the Genie space via CLI:

```bash
bash databricks/scripts/genie-mgmt.sh create
```

This calls `databricks genie create-space` with a serialized space definition — version 2 JSON with sorted table identifiers, 32-hex UUID IDs for sample questions, and instructions for interpreting anomaly scores and z-score thresholds. The CLI handles OAuth internally so no token materialization is needed.

When the Workflow fires, it asks Genie questions like:

> "What are the most recent errors for grizl-backend on route /api/chat in the last 30 minutes?"

Genie translates this to SQL, runs it against the `grizl.observability.*` tables, and returns a natural-language summary. That summary becomes the evidence section of the GitHub issue.

The Spark SQL fallback exists for when the Genie API is unavailable or the space is cold. The evidence is slightly less readable from Spark (`Row(service='grizl-backend', status_code=500, ...)` level detail), but the issue still gets created and the anomaly fields are still there.

## The Workflow: no webhook required

The Fabric version had a conceptual split: Activator fired a webhook to an external orchestrator service, which called Fabric Data Agent, which created the GitHub issue.

The Databricks version collapses that into a single Workflow notebook execution:

1. Query `grizl_recent_anomaly_signals` via SQL warehouse.
2. Ask Genie for evidence on the top signal.
3. Resolve the target GitHub repository from `repo_map_json`.
4. Classify remediation policy (code-actionable / operational / ambiguous).
5. Build the issue body with anomaly score, baseline, actual, service, route, deployment SHA, error signature, evidence text.
6. Create the GitHub issue.
7. If policy allows and `copilot_enabled=true`, assign Copilot via GraphQL.

All of this happens inside the notebook. The GitHub token is passed as a job parameter — not hardcoded, not in the workspace — and comes from GCP Secret Manager in the submission script.

The Copilot assignment uses the GraphQL `addAssigneesToAssignable` mutation with the bot node ID `BOT_kgDOC9w8XQ`, because the REST assignees endpoint does not accept GitHub App logins. This matches the pattern in `githubIssue.service.js`.

## What the E2E looks like

1. Insert spike data: 25 HTTP 500s with `deployment_sha='e2e_spike_sha'` landing in `raw_logs` with `ingest_timestamp = CURRENT_TIMESTAMP()`.

2. Insert baseline data: 6 time bins × 50 requests × 1 error, spread over the past 3 hours via `TIMESTAMP_SECONDS(UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - (bin_h * 30 * 60))`.

3. Query `grizl_recent_anomaly_signals` via SQL warehouse. Returns 3 signals: route latency (z=99.9, because 9-second response times against a 200ms baseline are unkind), backend error rate (z=5.5), error signature spike (z=2.3).

4. Submit the Workflow run.

5. 88 seconds later: GitHub issue `[Databricks Anomaly] Route Latency Anomaly (/api/chat)` appears with labels `observability`, `databricks-anomaly`, `critical`, Copilot and the repo owner assigned.

6. 54 seconds after that: Copilot opens a WIP pull request.

The production version runs every 5 minutes. When there are no anomalies, the notebook exits cleanly and nobody is disturbed. When there are anomalies, the issue arrives with the deployment SHA, the route, the z-score, the baseline, and the evidence.

That is the version of an alert I wanted to build.

## What I published

https://github.com/Metafiziks/grizl-databricks-observability

It includes:

- DLT Auto Loader pipeline for GCS export → Delta ingestion
- Unity Catalog SQL logical views (equivalent to KQL functions in the Fabric version)
- z-score anomaly signal views (equivalent to `series_decompose_anomalies()` in the Fabric version)
- Lakehouse Monitoring setup for data quality and drift metrics
- MLflow anomaly model example (documented; not used in production path)
- Databricks Workflow notebook: anomaly query + Genie evidence + GitHub issue + Copilot
- Asset Bundle config (`databricks.yml`) with all job parameters wired
- `genie-mgmt.sh` for CLI-driven Genie space creation and management
- Provisioning scripts, dry-run mode, config templates
- No live workspace IDs, no tokens, no secrets

## The difference

The Fabric version:

- Strong time-series KQL operators built for this kind of work
- Activator as a managed alert runtime
- Fabric Data Agent registered against the Eventhouse schema

The Databricks version:

- Universal SQL with time-bin arithmetic in place of time-series primitives
- Workflow scheduler as the trigger
- Genie as the natural-language evidence layer over Delta
- Spark SQL as the zero-configuration fallback

Neither version is strictly better. They are the same philosophy implemented in the plumbing available on each platform.

The philosophy is:

- Anomaly detection should live close to the data.
- Incident payloads should carry numeric evidence.
- GitHub issues should read like a forensic report.
- Copilot assignment should be gated by policy, not optimism.
- The system should run unattended, in production, without anyone needing to ask it a dashboard question first.

The system can now snitch on production from two different cloud platforms simultaneously.

I am genuinely unsure if this is a strength or a warning sign.

Either way: the incident arrives with fingerprints.

Good.
