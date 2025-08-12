from flask import Flask, request, render_template, send_file, flash, redirect, url_for
import os
import json
import csv
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Required for flash messages

# GCP configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')
PUBSUB_TOPIC_FOR_SQL_IMPORT = os.environ.get('PUBSUB_TOPIC_FOR_SQL_IMPORT', 'sql-import-topic')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')

# Clients
storage_client = storage.Client()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_FOR_SQL_IMPORT)

# Cloud Function URL (replace with your deployed function endpoint)
RUN_ANALYSIS_URL = os.environ.get(
    "RUN_ANALYSIS_URL",
    "https://REGION-PROJECT_ID.cloudfunctions.net/run_analysis"
)

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


def create_table_from_csv_if_not_exists(table_name, csv_path):
    """Create BigQuery table from uploaded CSV if it doesn't exist."""
    ensure_dataset_exists(BIGQUERY_DATASET)
    table_ref = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    try:
        bq_client.get_table(table_ref)
        print(f"Table {table_ref} already exists.")
    except Exception:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_EMPTY
        )
        with open(csv_path, "rb") as source_file:
            load_job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)
        load_job.result()
        print(f"Table {table_ref} created from CSV.")


def run_analysis(table_name):
    """Runs a fresh analysis query, saves results to GCS and BigQuery."""
    ensure_dataset_exists(BIGQUERY_DATASET)

    try:
        bq_client.get_table(f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}")
    except Exception:
        raise ValueError(f"BigQuery table {table_name} not found.")

    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}` LIMIT 10"
    results = bq_client.query(query).result()

    local_csv = f"/tmp/{table_name}_results.csv"
    with open(local_csv, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        headers = [field.name for field in results.schema]
        writer.writerow(headers)
        for row in results:
            writer.writerow(list(row.values()))

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(local_csv)

    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_TRUNCATE"
    )
    bq_client.query(query, job_config=job_config).result()

    return table_name

@app.route('/', methods=['GET', 'POST'])
def index():
    message = ""
    error = ""

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        if uploaded_file:
            try:
                file_path = f"/tmp/{uploaded_file.filename}"
                uploaded_file.save(file_path)

                # Upload to GCS
                bucket = storage_client.bucket(BUCKET_NAME)
                blob = bucket.blob(uploaded_file.filename)
                blob.upload_from_filename(file_path)

                # If SQL file, trigger import
                if uploaded_file.filename.endswith('.sql'):
                    message_data = {'name': uploaded_file.filename, 'bucket': BUCKET_NAME}
                    publisher.publish(topic_path, data=json.dumps(message_data).encode('utf-8'))

                table_name = os.path.splitext(uploaded_file.filename)[0]

                # Call Cloud Function to auto-create table + run analysis
                try:
                    resp = requests.get(f"{RUN_ANALYSIS_URL}?table={table_name}", timeout=60)
                    if resp.status_code == 200:
                        message = resp.text
                    else:
                        error = f"Analysis failed: {resp.text}"
                except Exception as e:
                    error = f"Error calling run_analysis: {str(e)}"

            except Exception as e:
                error = f"File upload failed: {str(e)}"

    return render_template('index.html', message=message, error=error)


@app.route('/download/<filename>')
def download_file(filename):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{filename}")

    if not blob.exists():
        flash(f"File {filename} not found in analysis_results folder.", "error")
        return redirect(url_for('index'))

    temp_path = f"/tmp/{filename}"
    blob.download_to_filename(temp_path)
    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=filename)


@app.route('/download_bq')
def download_bq():
    table_name = request.args.get("table")
    if not table_name:
        flash("Missing ?table parameter.", "error")
        return redirect(url_for('index'))

    try:
        run_analysis(table_name)
    except ValueError as e:
        flash(str(e), "error")
        return redirect(url_for('index'))
    except Exception as e:
        flash(f"Unexpected error: {e}", "error")
        return redirect(url_for('index'))

    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis`"
    results = bq_client.query(query).result()

    temp_path = f"/tmp/{table_name}_analysis.csv"
    with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        headers = [field.name for field in results.schema]
        writer.writerow(headers)
        for row in results:
            writer.writerow(list(row.values()))

    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=f"{table_name}_analysis.csv")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))




