#!/bin/bash
PROJECT_ID="data-analysis-webapp"
BUCKET_NAME="data-analysis-upload-1000"
BQ_DATASET="analysis_dataset"
PUBSUB_TOPIC="sql-import-topic"

gcloud config set project $PROJECT_ID

echo "Creating GCS bucket..."
gcloud storage buckets create gs://$BUCKET_NAME --location=asia-southeast1

echo "Creating BigQuery dataset..."
bq --location=US mk --dataset $PROJECT_ID:$BQ_DATASET

echo "Creating Pub/Sub topic..."
gcloud pubsub topics create $PUBSUB_TOPIC
