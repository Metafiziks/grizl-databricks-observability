#!/usr/bin/env bash
# gcs-subscription.sh — create the Pub/Sub Cloud Storage export subscription
# that feeds log messages into GCS for Databricks Auto Loader.
#
# This subscription runs in parallel to the existing pull subscription consumed
# by grizl-log-forwarder. No changes to grizl-log-forwarder are required.
#
# Usage:
#   GCS_BUCKET=<bucket> GCP_PROJECT_ID=<project> PUBSUB_TOPIC=<topic> bash gcs-subscription.sh
#   bash gcs-subscription.sh <bucket>            (reads GCP_PROJECT_ID and PUBSUB_TOPIC from env)
#   bash gcs-subscription.sh <bucket> dry-run    (print commands without running)
#
# Prerequisites:
#   gcloud auth login
#   gcloud config set project <GCP_PROJECT_ID>
#   Set GCP_PROJECT_ID and PUBSUB_TOPIC in databricks/config/grizl.databricks.env

set -euo pipefail

BUCKET="${1:-${GCS_BUCKET:-}}"
ACTION="${2:-apply}"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-}"
PUBSUB_TOPIC="${PUBSUB_TOPIC:-}"
SUBSCRIPTION_NAME="${GCS_EXPORT_SUBSCRIPTION:-grizl-logs-gcs-export}"

if [[ -z "$BUCKET" ]]; then
  echo "Error: GCS bucket name is required."
  echo "Usage: GCS_BUCKET=<bucket> bash $0   OR   bash $0 <bucket>"
  exit 1
fi

if [[ -z "$GCP_PROJECT_ID" ]]; then
  echo "Error: GCP_PROJECT_ID is not set. Export it or add to grizl.databricks.env."
  exit 1
fi

if [[ -z "$PUBSUB_TOPIC" ]]; then
  echo "Error: PUBSUB_TOPIC is not set. Export it or add to grizl.databricks.env."
  exit 1
fi

echo "GCS export subscription setup"
echo "  Project:      $GCP_PROJECT_ID"
echo "  Topic:        $PUBSUB_TOPIC"
echo "  Subscription: $SUBSCRIPTION_NAME"
echo "  Bucket:       gs://$BUCKET"
echo "  Action:       $ACTION"
echo ""

if [[ "$ACTION" == "dry-run" ]]; then
  echo "[DRY RUN] Would run:"
  echo ""
  echo "  gsutil mb -l us-central1 gs://$BUCKET"
  echo ""
  echo "  gcloud pubsub subscriptions create $SUBSCRIPTION_NAME \\"
  echo "    --topic=$PUBSUB_TOPIC --project=$GCP_PROJECT_ID \\"
  echo "    --cloud-storage-bucket=$BUCKET \\"
  echo "    --cloud-storage-file-prefix=logs/ \\"
  echo "    --cloud-storage-file-suffix=.jsonl \\"
  echo "    --cloud-storage-max-duration=60s \\"
  echo "    --cloud-storage-output-format=text"
  echo ""
  echo "  # Grant Databricks service account objectViewer on the bucket:"
  echo "  gsutil iam ch serviceAccount:<DATABRICKS_GSA>@$GCP_PROJECT_ID.iam.gserviceaccount.com:objectViewer gs://$BUCKET"
  exit 0
fi

# Create the bucket if it doesn't exist
if ! gsutil ls -b "gs://$BUCKET" &>/dev/null; then
  echo "Creating bucket gs://$BUCKET ..."
  gsutil mb -l us-central1 "gs://$BUCKET"
else
  echo "Bucket gs://$BUCKET already exists."
fi

# Create the Cloud Storage export subscription
if gcloud pubsub subscriptions describe "$SUBSCRIPTION_NAME" --project="$GCP_PROJECT_ID" &>/dev/null; then
  echo "Subscription $SUBSCRIPTION_NAME already exists."
else
  echo "Creating subscription $SUBSCRIPTION_NAME ..."
  gcloud pubsub subscriptions create "$SUBSCRIPTION_NAME" \
    --topic="$PUBSUB_TOPIC" \
    --project="$GCP_PROJECT_ID" \
    --cloud-storage-bucket="$BUCKET" \
    --cloud-storage-file-prefix="logs/" \
    --cloud-storage-file-suffix=".jsonl" \
    --cloud-storage-max-duration="60s" \
    --cloud-storage-output-format="text"
  echo "Subscription created."
fi

echo ""
echo "Done. Messages from $PUBSUB_TOPIC will export to gs://$BUCKET/logs/*.jsonl every 60s."
echo ""
echo "Next steps:"
echo "  1. Grant your Databricks service account objectViewer on the bucket:"
echo "     gsutil iam ch serviceAccount:<DATABRICKS_GSA>@$GCP_PROJECT_ID.iam.gserviceaccount.com:objectViewer gs://$BUCKET"
echo "  2. Set gcs_logs_path in databricks/databricks.yml: gs://$BUCKET/logs/"
echo "  3. Deploy: databricks bundle deploy --target dev"
