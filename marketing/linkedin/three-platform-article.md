# Part VIII: I Made the Same Observability Pipeline Run on Three Data Platforms and Now GitHub Issues Arrive in Triplicate

Subtitle: Fabric, Databricks, and Snowflake. One architecture. Three sets of nouns. Every possible mistake made exactly once per platform, and one made three times because I did not learn.

---

Three platforms.

Three articles.

One GitHub issue arriving from each of them.

I am not going to pretend this was the plan from the beginning. The plan from the beginning was to build the Fabric version, feel satisfied, and move on.

That was Part V.

Then I built it on Databricks. That was Part VI.

Then I built it on Snowflake. That was Part VII.

And now, standing in the wreckage of eight weeks of cloud platform tourism, I find myself in a position to tell you: here is how all three work, here is what is different, here is what broke, and here is proof that when the Snowflake alert fires, the GitHub issue appears with postmortem citations within ninety seconds.

The proof is GitHub issue [#263](https://github.com/Metafiziks/grizl-backend/issues/263). It was created by a live end-to-end test while I was writing this sentence.

Let us go.

## The premise, for people who skipped the last three articles

The system does this:

1. Application telemetry lands in a cloud data platform.
2. Anomaly detection runs over the data.
3. When something is genuinely weird, an alert fires.
4. The alert creates a GitHub issue with enough forensic context that a human — or Copilot — can open the issue and immediately understand what happened, what route was involved, what the baseline looked like before the betrayal, and whether there is a relevant postmortem.
5. If policy allows (scoped, code-actionable, above severity threshold, no guardrail conflicts), GitHub Copilot gets assigned to the issue and opens a WIP pull request.

No dashboards waiting to be checked. No Slack alerts that say "error rate elevated, please investigate." No tickets that open with "something seems wrong."

Just a GitHub issue that reads like a forensic report, arriving in the repository where the fix needs to live, assigned to the agent most likely to start working on it.

The question I kept asking was: which platform makes this easiest to build? Can this architecture survive translation? What does each platform get right that the others do not?

The answers are: it depends, yes, and more than I expected.

## The shared skeleton

Across all three platforms, the pipeline is structurally identical:

```
Cloud Logging → Pub/Sub → [Ingestion layer] → [Raw log table]
                                                      ↓
                               [Logical views: HTTP, latency, deployments]
                                                      ↓
                               [Anomaly signal views: z-score or ML]
                                                      ↓
                               [Alert / Scheduler: every 5 minutes]
                                                      ↓
                               [Evidence layer: natural-language queries]
                                                      ↓
                               [Incident orchestrator: GitHub issue + Copilot]
```

The application code has not changed across any version. The Cloud Logging sink is the same. The Pub/Sub topic is the same. The GitHub issue format is the same. The Copilot assignment GraphQL mutation is the same: `addAssigneesToAssignable` with node ID `BOT_kgDOC9w8XQ`, because the REST assignees endpoint does not accept GitHub App logins.

Where the platforms diverge is in every box between "Pub/Sub" and "GitHub issue."

## Ingestion: the one thing that got easier on every port

**Fabric**: The log forwarder sends events to Fabric Eventstream, which writes to Eventhouse `RawLogs`. One configuration change in the forwarder. No new cloud infrastructure.

**Databricks**: One command added a Cloud Storage export subscription to the existing Pub/Sub topic:

```bash
gcloud pubsub subscriptions create grizl-logs-gcs-export \
  --topic=grizl-log-topic \
  --cloud-storage-bucket=<bucket> \
  --cloud-storage-file-prefix=logs/ \
  --cloud-storage-max-duration=60s \
  --cloud-storage-output-format=text
```

Auto Loader reads from the same GCS bucket. The forwarder does not know. The applications do not know. The pull subscription in the original forwarder keeps running. One topic, two subscribers, zero drama.

**Snowflake**: Reads from the same GCS bucket the Databricks version created. Snowpipe gets a GCS notification integration, a storage integration, and a `COPY INTO` pipeline that activates on object-finalize events. Same files. Third platform reading them.

The ingestion story across all three: one Pub/Sub topic, one GCS bucket, three platforms consuming the same data, nothing broken, nobody told.

The exception was Snowpipe AUTO_INGEST on an AWS-deployed Snowflake account.

### The Snowpipe exception

Snowflake assigns each account a GCP service account for its notification integration. On an AWS-deployed Snowflake account (which is what I have), the assigned service account is in a Snowflake-managed GCP project, not a user-visible one.

The error:

```
Error 090040: PERMISSION_DENIED: request from service account
kg6j40000@awsuseast2-a5c7.iam.gserviceaccount.com to subscribe
to subscription projects/<PROJECT>/subscriptions/grizl-snowpipe-sf-sub
was rejected
```

I tried: subscription-level IAM. Project-level `pubsub.subscriber`. Project-level `pubsub.admin`. A fresh subscription. Recreating the integration as ACCOUNTADMIN. All produced the same error.

The root cause is an authentication path issue in Snowflake's infrastructure for cross-cloud accounts that requires a Snowflake support case, not an IAM fix.

The solution was a `TASK_COPY_INTO` that runs on a one-minute schedule and uses Snowflake's built-in file deduplication instead of event-driven notifications:

```sql
CREATE OR REPLACE TASK GRIZL.OBSERVABILITY.TASK_COPY_INTO
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '1 MINUTE'
AS
COPY INTO GRIZL.OBSERVABILITY.RAW_LOGS ... FROM @GCS_LOGS_STAGE
FILE_FORMAT = (TYPE = 'JSON');
```

One-minute polling. Same data. Works on any Snowflake deployment. The Snowpipe DDL is still in the repo for when the support case closes.

## Anomaly detection: three different answers to the same question

The question is: is the current metric value meaningfully outside the normal range, and by how much?

**Fabric** answers this with `series_decompose_anomalies()` — a native Eventhouse time-series operator that handles seasonality, trend decomposition, and residual scoring. You pass it a time-series column and a threshold. It returns `anomaly_score` directly. Nothing to compute manually.

**Databricks** answers this with SQL z-score arithmetic:

```sql
(actual - AVG(baseline)) / NULLIF(STDDEV(baseline), 0) AS anomaly_score
```

`FLOOR(UNIX_TIMESTAMP(ts) / 300) * 300` buckets events into 5-minute bins. The baseline window is everything older than 15 minutes in the last 2 days. If the standard deviation is zero — not enough data variance to make a meaningful comparison — the denominator is null and the row disappears. This is correct behavior.

The SQL is readable. Any engineer can understand it at 2 AM. That is not a small thing.

**Snowflake** has both paths.

The default path is the same z-score SQL as Databricks, implemented over Dynamic Tables instead of plain views:

```sql
CREATE OR REPLACE DYNAMIC TABLE GRIZL.OBSERVABILITY.DT_HTTP_ERROR_RATE
  TARGET_LAG = '5 minutes'
...
```

`TARGET_LAG = '5 minutes'` means the materialized baseline is pre-computed and at most 5 minutes stale. The anomaly signal view reads the cached result. The Alert gets a fast answer without rescanning `RAW_LOGS` on every evaluation.

The upgrade path is `SNOWFLAKE.ML.ANOMALY_DETECTION` — a trained time-series model that returns `IS_ANOMALY`, `DISTANCE`, `FORECAST`, `LOWER_BOUND`, and `UPPER_BOUND`. It learns from a Dynamic Table as training input. Once training data accumulates, the ML path replaces the z-score arithmetic and you stop doing the statistics yourself.

Summary:

| Platform | Method | Setup |
|---|---|---|
| Fabric | `series_decompose_anomalies()` | Zero — native KQL |
| Databricks | SQL z-score | SQL you write |
| Snowflake (default) | SQL z-score over Dynamic Tables | SQL you write + lag config |
| Snowflake (ML) | `ANOMALY_DETECTION` model | Train once, upgrade later |

## Evidence: the part that actually changes how fast you close the issue

This is the most meaningful difference between the three platforms.

**Fabric**: Fabric Data Agent. Natural language to KQL over the Eventhouse database. You ask it a question in plain English. It returns a plain-English answer. No semantic model file required.

**Databricks**: Genie. Natural language to SQL over Delta tables. Similar premise. Answers questions about the `grizl.observability.*` tables. Spark SQL fallback when Genie is unavailable.

**Snowflake**: Cortex Agents with two tools.

```json
{
  "tools": [
    { "tool_spec": { "type": "cortex_analyst_text_to_sql", "name": "cortex_analyst_grizl" } },
    { "tool_spec": { "type": "cortex_search", "name": "cortex_search_grizl_knowledge" } }
  ]
}
```

Tool one is Cortex Analyst — natural language to SQL over the telemetry tables. It answers "what happened."

Tool two is Cortex Search over `GRIZL.KNOWLEDGE.ARTICLES` — a table of postmortems, runbooks, and architecture notes indexed as a Search Service. It answers "has this happened before and what fixed it."

The GitHub issue that arrives from Snowflake contains both layers:

> Analyst: "The most common error in the last 30 minutes is RuntimeError:/api/chat with 18 occurrences. All errors are associated with deployment SHA `e2e_spike_sha`."
>
> Search: "**Related postmortem**: PM-2026-04-12 grizl-backend /api/chat timeout regression. Root cause: OpenAI client connection pool exhaustion under high concurrency. Remediation: increase MAX_CONNECTIONS."

"This looks like the April timeout regression" is more useful than a SQL row dump at 3 AM. It is the difference between an alert that tells you a fact and an alert that tells you where to start.

Fabric and Databricks do not have postmortem search. Snowflake does. That is the most concrete advantage of the Snowflake version.

## The alert path: three different architectures for the same outcome

**Fabric**: Fabric Activator monitors a condition on a Reflex rule schedule. When the condition is met, it POSTs to an external incident orchestrator webhook. The orchestrator calls Fabric Data Agent, creates the GitHub issue, assigns Copilot.

**Databricks**: A Databricks Workflow runs every five minutes. The notebook queries `grizl_recent_anomaly_signals` via SQL warehouse, asks Genie for evidence, creates the GitHub issue directly, assigns Copilot. No webhook. No external service. The Workflow is the orchestrator.

**Snowflake**: A Snowflake Alert evaluates every five minutes:

```sql
CREATE OR REPLACE ALERT GRIZL.OBSERVABILITY.GRIZL_ANOMALY_ALERT
  WAREHOUSE = GRIZL_WH
  SCHEDULE  = '5 MINUTES'
  IF (EXISTS (SELECT 1 FROM GRIZL_RECENT_ANOMALY_SIGNALS WHERE ANOMALY_SCORE >= 1.5 LIMIT 1))
  THEN CALL GRIZL.OBSERVABILITY.SP_NOTIFY_ANOMALY_INCIDENT();
```

`SP_NOTIFY_ANOMALY_INCIDENT` is a Python stored procedure that reads two Snowflake secrets via `_snowflake.get_generic_secret_string()` — the webhook URL and a Bearer auth token — and POSTs to the external orchestrator. The external orchestrator creates the GitHub issue and assigns Copilot.

Snowflake returns to the webhook pattern from Fabric. Databricks is the only version that is fully self-contained. Neither approach is wrong.

## The disasters

Every platform produced exactly one category of disaster that the others did not.

### Fabric: the guardrail conflict that blocked Copilot

The Fabric version's policy classifier marks `HIGH_LATENCY` signals as not safe for Copilot assignment, because latency signals from Fabric telemetry do not always include a specific route. "Latency is up" without a route target is not actionable by a coding agent.

The Snowflake version reverses this. Snowflake anomaly signals always include a route — the signal views are grouped by `SERVICE` and `ROUTE` explicitly. So `HIGH_LATENCY` in the Snowflake orchestrator is in `SAFE_ACTIONABLE_TYPES`. Same architecture, different policy, because the signal payload is more precise.

The lesson: the remediation policy is not generic. It has to know what the signal actually contains.

### Databricks: the Python that became a markdown poem

Databricks Python notebooks separate cells with `# COMMAND ----------`. If a cell starts with `# MAGIC %md`, everything in that cell is rendered as Markdown.

Including, apparently, Python code.

The anomaly signals Workflow notebook had section headers like `# MAGIC %md ## 1. Query the anomaly signal view` followed immediately by Python functions — in the same cell. Databricks rendered all of it as a markdown code block. The functions were decorative. The notebook completed in 15 seconds with `SUCCESS`, having queried nothing, detected nothing, and created zero issues.

The fix: `# COMMAND ----------` between every header and its code block. Seven inserts. The notebook then ran for 88 seconds, which is how I knew it was actually working. The GitHub issue appeared 54 seconds later.

"The Python became a markdown poem" is a sentence that now lives in my vocabulary for explaining how software development works.

### Snowflake: the trailing newline that caused a 401

The Bearer auth secret for the Snowflake SP → orchestrator connection was generated with:

```python
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

`print()` appends a newline. The secret was stored in GCP Secret Manager as `<token>\n` — 44 bytes.

Cloud Run mounts `secretKeyRef` values as-is. The env var in production was `<token>\n`.

The Snowflake Python SP read the secret with `.strip()`. The test script read it with `.strip()`. Both sent 43-byte tokens. The backend compared against the 44-byte env var. `crypto.timingSafeEqual` exits immediately on length mismatch.

The fix: `.trim()` the env var before comparison:

```javascript
const expected = (process.env.SNOWFLAKE_ALERT_WEBHOOK_SECRET || '').trim();
```

One character. Two deploys. Forty minutes. These are the moments that build character.

### The bonus disaster that happened on all three

Cloud Run's `gcloud run services update` has two flags that look similar and behave very differently:

- `--update-env-vars`: merges with existing env vars
- `--set-env-vars`: replaces all env vars

A previous session used `--set-env-vars` and `--set-secrets` to add two Snowflake-specific vars. The revision that deployed had exactly those two vars. The 120 previously-configured env vars were gone.

The service crashed on startup with `OpenAIError: Missing credentials` because `ai-router.js` instantiates the OpenAI client at module load time.

The fix: read the full env var set from the last working revision, write a Python script that reconstructs the complete `--update-env-vars` and `--update-secrets` call, run it. The restore took twelve minutes to figure out and thirty seconds to execute.

The lesson: always use `--update-*`, never `--set-*`, unless you mean to replace everything.

## The live proof

After all of the above, the full chain was tested end-to-end:

1. `GRIZL_ANOMALY_ALERT` resumed in live Snowflake.
2. `SP_NOTIFY_ANOMALY_INCIDENT` applied with Bearer auth header.
3. `grizl-backend` deployed with `SNOWFLAKE_INCIDENT_ORCHESTRATOR_ENABLED=true`.
4. Test POST to `POST /api/snowflake/incidents` with a synthetic signal payload.

Response:

```json
{
  "status": "accepted",
  "incident": {
    "alertName": "route_latency_p95 anomaly on /api/orders",
    "severity": "critical",
    "anomalyType": "HIGH_LATENCY"
  },
  "issue": {
    "url": "https://github.com/Metafiziks/grizl-backend/issues/263",
    "number": 263
  },
  "policy": {
    "action": "copilot_candidate",
    "safeForCopilot": true
  },
  "copilotAction": {
    "status": "assigned",
    "assignee": "Copilot"
  }
}
```

HTTP 202. GitHub issue [#263](https://github.com/Metafiziks/grizl-backend/issues/263). Copilot assigned.

That is the end-to-end test. Three platforms live. All pointing at the same GitHub repository. One architecture.

## What each platform actually got right

| | Fabric | Databricks | Snowflake |
|---|---|---|---|
| Anomaly detection | `series_decompose_anomalies()` — native, no math required | SQL z-score — readable by anyone at 2 AM | Cortex ML — trains itself once data accumulates |
| Evidence layer | Fabric Data Agent — zero config | Genie — zero config | Cortex Agents — two tools, postmortem search |
| Alert runtime | Activator — managed, policy-driven | Workflow — self-contained, no webhook | Alert + SP — clean separation |
| Ingestion | Eventstream — managed pipeline | Auto Loader — schema evolution automatic | Dynamic Tables — materialized baseline included |
| Best feature | Time-series primitives in KQL | Entire pipeline in one notebook | Postmortem citations in the GitHub issue |
| Worst experience | Policy classification nuance | The Python became markdown | Trailing newline. One byte. |

None of them is better in all dimensions. They are the same philosophy — anomaly detection close to the data, incident payloads with receipts, Copilot only when the signal is actionable — implemented with whatever each platform makes easy.

## What I published

**Fabric** (Part V):
https://github.com/Metafiziks/grizl-fabric-observability

**Databricks** (Part VI):
https://github.com/Metafiziks/grizl-databricks-observability

**Snowflake** (Part VII):
https://github.com/Metafiziks/grizl-snowflake-observability

Each repo includes:

- Ingestion setup (Eventstream / Auto Loader / Snowpipe + task fallback)
- Anomaly signal views
- Alert / Workflow / Scheduler config
- Evidence layer setup
- Incident orchestrator integration
- Provisioning scripts, dry-run mode, config templates
- No live workspace identifiers, tokens, or connection strings

The backend that receives the webhooks and creates the GitHub issues:
https://github.com/Metafiziks/grizl-backend

## The closing position

I have now built the same pipeline three times.

Each time I built it, the GitHub issue arrived cleaner, with more evidence, with better postmortem context. Each time I built it, I understood the previous version more clearly by watching how the new platform handled the same problem differently.

I am not sure that was the point when I started.

But the incident arrives with fingerprints now, from three platforms simultaneously, with postmortem citations on the Snowflake path, and Copilot on the PR ninety seconds later.

That is what I was trying to build.

Good.
