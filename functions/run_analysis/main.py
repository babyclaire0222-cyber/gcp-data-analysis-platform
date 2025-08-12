import os
import csv
from google.cloud import bigquery, storage
from flask import Request

PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')

bq_client = bigquery.Client()
storage_client = storage.Client()

# Define a fixed schema for Looker Studio table
FIXED_SCHEMA = [
    bigquery.SchemaField("column1", "STRING"),
    bigquery.SchemaField("column2", "INTEGER"),
    bigquery.SchemaField("column3", "FLOAT"),
    bigquery.SchemaField("column4", "TIMESTAMP")
]

def ensure_dataset_exists(dataset_id):
    """Create dataset if it doesn't already exist."""
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        bq_client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

def ensure_table_exists(dataset_id, table_name, schema):
    """Create table with given schema if it doesn't already exist."""
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    try:
        bq_client.get_table(table_id)
    except Exception:
        table = bigquery.Table(table_id, schema=schema)
        table = bq_client.create_table(table)
        print(f"Table {table_id} created with fixed schema.")

def run_analysis(request: Request):
    table_name = request.args.get('table')

    if not table_name:
        return "Missing 'table' query parameter. Example: ?table=my_table", 400

    # Make sure dataset and table exist
    ensure_dataset_exists(BIGQUERY_DATASET)
    ensure_table_exists(BIGQUERY_DATASET, table_name, FIXED_SCHEMA)

    # Build query dynamically
    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` LIMIT 10"
    query_job = bq_client.query(query)
    results = query_job.result()

    # Save results to CSV in /tmp
    result_file = "/tmp/results.csv"
    with open(result_file, "w") as f:
        headers = [field.name for field in results.schema]
        f.write(",".join(headers) + "\n")
        for row in results:
            f.write(",".join([str(x) for x in row.values()]) + "\n")

    # Upload CSV to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(result_file)

    # Load into fixed-schema BigQuery table for Looker Studio
    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    job_config = bigquery.LoadJobConfig(
        schema=FIXED_SCHEMA,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(result_file, "rb") as source_file:
        load_job = bq_client.load_table_from_file(source_file, destination_table, job_config=job_config)
        load_job.result()

    return (
        f"Analysis complete for table '{table_name}'.\n"
        f"Results saved to GCS and BigQuery table '{table_name}_analysis'.\n"
        f"Connect Looker Studio to: {destination_table}"
    )


