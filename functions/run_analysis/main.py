import os
from google.cloud import bigquery, storage
from flask import Request

PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')

bq_client = bigquery.Client()
storage_client = storage.Client()

def _dataset_location():
    # Read actual dataset location (safer than assuming)
    ds = bq_client.get_dataset(f"{PROJECT_ID}.{BIGQUERY_DATASET}")
    return ds.location or BIGQUERY_LOCATION

# Define a fixed schema for Looker Studio table (if needed)
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

    location = _dataset_location()

    # Query source table
    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` LIMIT 10"
    qcfg = bigquery.QueryJobConfig()
    qjob = bq_client.query(query, job_config=qcfg, location=location)  # <<< set location
    results = qjob.result()

    # Write temp CSV to /tmp and upload to GCS (unchanged) …

    # Load results into *_analysis table
    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    load_cfg = bigquery.LoadJobConfig(
        # if you’re enforcing a schema, keep it here; otherwise use autodetect:
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    with open(result_file, "rb") as fh:
        ljob = bq_client.load_table_from_file(fh, destination_table, job_config=load_cfg, location=location)  # <<< set location
        ljob.result()

    return f"Analysis complete for '{table_name}'. Loaded into {destination_table}."
    
def create_table_from_csv_if_not_exists(table_name):
    """Creates a BigQuery table from a CSV in GCS if it doesn't exist."""
    table_ref = bq_client.dataset(BIGQUERY_DATASET).table(table_name)
    try:
        bq_client.get_table(table_ref)
        return  # Table already exists
    except Exception:
        print(f"Table {table_name} not found. Searching for CSV in GCS...")

    # Possible CSV paths
    possible_paths = [
        f"{table_name}.csv",
        f"uploads/{table_name}.csv",
        f"data/{table_name}.csv",
        f"raw/{table_name}.csv"
    ]

    bucket = storage_client.bucket(BUCKET_NAME)
    csv_uri = None

    for path in possible_paths:
        blob = bucket.blob(path)
        if blob.exists():
            csv_uri = f"gs://{BUCKET_NAME}/{path}"
            print(f"Found CSV at {path}")
            break

    if not csv_uri:
        raise ValueError(f"No CSV file found in GCS for table {table_name}")

    # Load into BigQuery with autodetect schema
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    load_job = bq_client.load_table_from_uri(csv_uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Created BigQuery table {table_name} from CSV: {csv_uri}")

def run_analysis(request: Request):
    table_name = request.args.get('table')

    if not table_name:
        return "Missing 'table' query parameter. Example: ?table=my_table", 400

    # Ensure table exists (create if CSV available)
    create_table_from_csv_if_not_exists(table_name)

    # Query first 10 rows
    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` LIMIT 10"
    results = bq_client.query(query).result()

    # Save results to CSV in /tmp
    result_file = "/tmp/results.csv"
    with open(result_file, "w") as f:
        headers = [field.name for field in results.schema]
        f.write(",".join(headers) + "\n")
        for row in results:
            f.write(",".join([str(x) for x in row.values()]) + "\n")

    # Upload analysis results to GCS
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
