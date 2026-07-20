# Supporting LinkedIn Post

I ported the panic button to Databricks.

Built the same anomaly-detection-to-GitHub-issue pipeline from Part V — but replaced Fabric Eventhouse and KQL with Delta Lake and SQL z-scores, Fabric Activator with a Databricks Workflow, and Fabric Data Agent with Genie.

Ingestion: one `gcloud pubsub subscriptions create` command adds a Cloud Storage export subscription to the existing Pub/Sub topic. Auto Loader picks up the JSONL files. `raw_logs` lands in Delta / Unity Catalog. No changes to any application or Cloud Run service.

Detection: z-score anomaly views over 5-minute `FLOOR(UNIX_TIMESTAMP/300)*300` bins. 2-day rolling baseline. Standard deviation as the variance test. No time-series model. Just SQL that anyone can read at 2 AM.

Evidence: Genie (AI/BI) answers natural-language questions over the `grizl.observability.*` tables. Spark SQL fallback if Genie is unavailable.

Response: the Workflow notebook queries `grizl_recent_anomaly_signals`, asks Genie for evidence, builds the issue body with anomaly score, baseline, actual, service, route, deployment SHA, and creates the GitHub issue directly — no external webhook.

Copilot gets assigned when policy allows. Copilot opened a WIP PR 54 seconds after the issue appeared.

One thing I did not expect: the notebook's Python code was silently rendering as markdown for every test run because the code was inside `# MAGIC %md` cells. The notebook completed in 15 seconds, reported SUCCESS, and created zero issues. Adding `# COMMAND ----------` between the headers and the code fixed it. The notebook then took 88 seconds and the issue appeared.

"The Python became a markdown poem" is a debugging experience I am not looking to repeat.

Public sanitized package:

https://github.com/Metafiziks/grizl-databricks-observability

No live workspace IDs. No tokens. No ornamental MLflow model. Just Delta, SQL anomaly views, a Workflow that calls GitHub, and incidents that arrive with receipts.

Article below 👇

#Databricks #DeltaLake #MLflow #Observability #AnomalyDetection #GCP #GitHub #Copilot #DevOps #AIOps #DataEngineering
