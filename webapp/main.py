from flask import Flask, request, render_template, send_file
import os
import json
import csv
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1

app = Flask(__name__)

# GCP configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')
PUBSUB_TOPIC_FOR_SQL_IMPORT = os.environ.get('PUBSUB_TOPIC_FOR_SQL_IMPORT', 'sql-import-topic')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')

# Clients with location set
storage_client = storage.Client()
bq_client = bigquery.Client(location="asia-southeast1")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_FOR_SQL_IMPORT)

def ensure_dataset_exists(dataset_id):
    """Create dataset if it doesn't already exist."""
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"  # ✅ Set location explicitly
        bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created in asia-southeast1.")

def table_exists(table_id):
    """Check if BigQuery table exists."""
    try:
        bq_client.get_table(table_id)
        return True
    except Exception:
        return False

def run_analysis(table_name):
    """Runs a fresh analysis query, saves results to GCS and BigQuery."""
    ensure_dataset_exists(BIGQUERY_DATASET)

    base_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    analysis_table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"

    # Prefer analysis table if exists, otherwise use base table
    if table_exists(analysis_table_id):
        query_table = analysis_table_id
    elif table_exists(base_table_id):
        query_table = base_table_id
    else:
        raise ValueError(f"No table found: {base_table_id} or {analysis_table_id}")

    query = f"SELECT * FROM `{query_table}` LIMIT 10"
    results = bq_client.query(query).result()

    local_csv = f"/tmp/{table_name}_results.csv"
    with open(local_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        headers = [field.name for field in results.schema]
        writer.writerow(headers)
        for row in results:
            writer.writerow(list(row.values()))

    # Upload to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(local_csv)

    # Save results to `<table_name>_analysis` table
    job_config = bigquery.QueryJobConfig(
        destination=analysis_table_id,
        write_disposition="WRITE_TRUNCATE"
    )
    bq_client.query(query, job_config=job_config).result()

    return table_name

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))




