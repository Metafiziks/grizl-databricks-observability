# Databricks notebook source
# MAGIC %md
# MAGIC # GRIZL Custom Anomaly Model (MLflow + Unity Catalog)
# MAGIC
# MAGIC Demonstrates the "bring your own model" path for ML observability.
# MAGIC Lakehouse Monitoring handles drift/quality alerting natively via managed
# MAGIC statistical tests. This notebook adds a custom ML model layer for:
# MAGIC
# MAGIC - **Multivariate anomalies**: detect unusual combinations of error_rate,
# MAGIC   latency, and traffic volume that individually look normal
# MAGIC - **Composite scoring**: combine multiple signals into a single risk score
# MAGIC - **Feedback loop retraining**: update the model when incidents are resolved
# MAGIC - **Unity Catalog registration**: govern the model alongside the data
# MAGIC
# MAGIC ## Model: Isolation Forest over Lakehouse Monitor profiles
# MAGIC
# MAGIC Uses the `grizl.observability_monitors.raw_logs_profile` output (generated
# MAGIC by notebook 02) as training and scoring features. Each row represents one
# MAGIC time window + service slice with computed metric values.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - notebook 02 has run at least one refresh cycle
# MAGIC - `databricks-sdk`, `scikit-learn`, `mlflow` available on the cluster
# MAGIC - Unity Catalog ML model registry enabled

# COMMAND ----------

import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature

import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

mlflow.set_registry_uri("databricks-uc")

EXPERIMENT_NAME = "/Shared/grizl/observability/anomaly-model"
MODEL_NAME      = "grizl.observability.anomaly_detector"
MONITOR_SCHEMA  = "grizl.observability_monitors"

mlflow.set_experiment(EXPERIMENT_NAME)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load training features from Lakehouse Monitor profiles

profile_df = (
    spark.table(f"{MONITOR_SCHEMA}.raw_logs_profile")
    .filter("slice_key = 'service'")
    .select(
        "window_start_time",
        "slice_value",       # service name
        "num_rows",
        "error_rate",
        "http_5xx_rate",
        "p95_duration_ms",
        "p99_duration_ms",
        "deployment_sha_count",
    )
    .dropna(subset=["error_rate", "p95_duration_ms"])
    .toPandas()
)

print(f"Loaded {len(profile_df):,} profile rows across {profile_df['slice_value'].nunique()} services")
profile_df.head()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Feature engineering

FEATURE_COLS = [
    "num_rows",
    "error_rate",
    "http_5xx_rate",
    "p95_duration_ms",
    "p99_duration_ms",
    "deployment_sha_count",
]

X_train = profile_df[FEATURE_COLS].fillna(0).values
print(f"Training shape: {X_train.shape}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Train with MLflow tracking

CONTAMINATION  = 0.05   # expected anomaly fraction
N_ESTIMATORS   = 200
RANDOM_STATE   = 42

with mlflow.start_run(run_name="isolation-forest-v1") as run:
    mlflow.log_params({
        "model_type":     "IsolationForest",
        "contamination":  CONTAMINATION,
        "n_estimators":   N_ESTIMATORS,
        "feature_cols":   FEATURE_COLS,
        "training_rows":  len(X_train),
        "source_table":   f"{MONITOR_SCHEMA}.raw_logs_profile",
    })

    model_pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("iforest", IsolationForest(
            contamination=CONTAMINATION,
            n_estimators=N_ESTIMATORS,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )),
    ])

    model_pipeline.fit(X_train)

    # Score training set — -1 = anomaly, 1 = normal
    y_pred = model_pipeline.predict(X_train)
    scores = model_pipeline.score_samples(X_train)   # higher = more normal

    n_anomalies = (y_pred == -1).sum()
    mlflow.log_metrics({
        "train_anomaly_fraction": n_anomalies / len(y_pred),
        "train_n_anomalies":      int(n_anomalies),
        "score_mean":             float(np.mean(scores)),
        "score_std":              float(np.std(scores)),
    })

    # Log model with input/output signature
    sample_input  = pd.DataFrame(X_train[:5], columns=FEATURE_COLS)
    sample_output = pd.DataFrame({"anomaly_flag": y_pred[:5], "anomaly_score": scores[:5]})
    signature = infer_signature(sample_input, sample_output)

    mlflow.sklearn.log_model(
        sk_model=model_pipeline,
        artifact_path="model",
        signature=signature,
        registered_model_name=MODEL_NAME,
        input_example=sample_input,
    )

    print(f"Run ID:    {run.info.run_id}")
    print(f"Anomalies: {n_anomalies} / {len(y_pred)} ({n_anomalies/len(y_pred):.1%})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Score recent windows with the registered model

model_uri = f"models:/{MODEL_NAME}/latest"
loaded_model = mlflow.sklearn.load_model(model_uri)

recent_df = (
    spark.table(f"{MONITOR_SCHEMA}.raw_logs_profile")
    .filter("slice_key = 'service'")
    .filter("window_start_time >= current_timestamp() - INTERVAL 1 HOUR")
    .select("window_start_time", "slice_value", *FEATURE_COLS)
    .dropna(subset=["error_rate", "p95_duration_ms"])
    .toPandas()
)

if len(recent_df) > 0:
    X_recent = recent_df[FEATURE_COLS].fillna(0).values
    recent_df["anomaly_flag"]  = loaded_model.predict(X_recent)
    recent_df["anomaly_score"] = loaded_model.score_samples(X_recent)
    recent_df["is_anomaly"]    = recent_df["anomaly_flag"] == -1

    anomalies = recent_df[recent_df["is_anomaly"]].sort_values("anomaly_score")
    print(f"Recent anomalies: {len(anomalies)} / {len(recent_df)} windows")
    display(anomalies[["window_start_time", "slice_value", "anomaly_score",
                        "error_rate", "http_5xx_rate", "p95_duration_ms"]])
else:
    print("No recent profile data found. Ensure the monitor has refreshed at least once.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Write anomaly scores to Delta for SQL alerting and Workflow pickup

from pyspark.sql import functions as F

if len(recent_df) > 0:
    scored_spark = spark.createDataFrame(recent_df[[
        "window_start_time", "slice_value",
        "anomaly_flag", "anomaly_score", "is_anomaly",
        "error_rate", "http_5xx_rate", "p95_duration_ms",
    ]]).withColumnRenamed("slice_value", "service")

    (
        scored_spark
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("grizl.observability.ml_anomaly_scores")
    )

    print("Scores written to grizl.observability.ml_anomaly_scores")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. MLflow trace logging (for LLM / agent calls, optional)
# MAGIC
# MAGIC If GRIZL uses LLM/agent calls (e.g. Mosaic AI, Genie, or an external LLM),
# MAGIC enable MLflow Tracing to capture latency, token usage, and tool calls:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC mlflow.set_experiment("/Shared/grizl/agent-traces")
# MAGIC
# MAGIC with mlflow.start_span(name="grizl-agent-call") as span:
# MAGIC     span.set_inputs({"query": user_question})
# MAGIC     result = my_agent.invoke(user_question)
# MAGIC     span.set_outputs({"answer": result})
# MAGIC     span.set_attributes({"tokens_used": result.usage.total_tokens})
# MAGIC ```
# MAGIC
# MAGIC Traces are stored in the MLflow experiment and can be queried via:
# MAGIC ```python
# MAGIC traces = mlflow.search_traces(experiment_names=["/Shared/grizl/agent-traces"])
# MAGIC ```
