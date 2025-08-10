import os
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

def run_analysis(request: Request):
    table_name = request.args.get('table')

    if not table_name:
        return "Missing 'table' query parameter. Example: ?table=my_table", 400

    # Build query dynamically
    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` LIMIT 10"
    query_job = bq_client.query(query)
    results = query_job.result()

    # Save results to CSV in /tmp
    result_file = "/tmp/results.csv"
    with open(result_file, "w") as f:
        # Write header
        headers = [field.name for field in results.schema]
        f.write(",".join(headers) + "\n")

        # Write rows
        for row in results:
            f.write(",".join([str(x) for x in row.values()]) + "\n")

    # Upload CSV to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(result_file)

    # Load into fixed-schema BigQuery table for Looker Studio
    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    job_config = bigquery.LoadJobConfig(
        schema=FIXED_SCHEMA,  # enforce schema
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # skip header
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    with open(result_file, "rb") as source_file:
        load_job = bq_client.load_table_from_file(source_file, destination_table, job_config=job_config)
        load_job.result()  # Wait for the load to finish

    return (
        f"Analysis complete for table '{table_name}'.\n"
        f"Results saved to GCS and BigQuery table '{table_name}_analysis'.\n"
        f"Connect Looker Studio to: {destination_table}"
    )

