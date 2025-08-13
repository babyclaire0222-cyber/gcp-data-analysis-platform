from flask import Flask, request, render_template, send_file
import os
import json
import csv
import requests
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1

app = Flask(__name__)

# GCP configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')
PUBSUB_TOPIC_FOR_SQL_IMPORT = os.environ.get('PUBSUB_TOPIC_FOR_SQL_IMPORT', 'sql-import-topic')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')
RUN_ANALYSIS_URL = os.environ.get('RUN_ANALYSIS_URL', '')  # Cloud Function URL

# Clients
storage_client = storage.Client()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_FOR_SQL_IMPORT)


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


def create_table_from_csv_if_not_exists(table_name, file_path):
    """Create a BigQuery table from a CSV file if it doesn't already exist."""
    ensure_dataset_exists(BIGQUERY_DATASET)
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    try:
        bq_client.get_table(table_id)
        print(f"Table {table_name} already exists, skipping creation.")
    except Exception:
        print(f"Creating table {table_name} from CSV...")
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
        )
        with open(file_path, "rb") as source_file:
            load_job = bq_client.load_table_from_file(source_file, table_id, job_config=job_config)
        load_job.result()
        print(f"Table {table_name} created successfully.")


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

                table_name = os.path.splitext(uploaded_file.filename)[0]

                # Auto-create BigQuery table if CSV
                if uploaded_file.filename.endswith('.csv'):
                    try:
                        create_table_from_csv_if_not_exists(table_name, file_path)
                        message = f"Uploaded {uploaded_file.filename} and ensured table {table_name} exists."
                    except Exception as e:
                        error = f"Failed to create table from CSV: {str(e)}"
                        return render_template('index.html', message=message, error=error)

                # If SQL file, trigger import
                if uploaded_file.filename.endswith('.sql'):
                    message_data = {'name': uploaded_file.filename, 'bucket': BUCKET_NAME}
                    publisher.publish(topic_path, data=json.dumps(message_data).encode('utf-8'))

                # Call Cloud Function for analysis
                if RUN_ANALYSIS_URL:
                    try:
                        resp = requests.get(f"{RUN_ANALYSIS_URL}?table={table_name}", timeout=60)
                        if resp.status_code == 200:
                            message += "<br>Analysis complete:<br>" + resp.text.replace("\n", "<br>")
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
        return f"File {filename} not found in analysis_results folder.", 404

    temp_path = f"/tmp/{filename}"
    blob.download_to_filename(temp_path)
    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=filename)


@app.route('/download_bq')
def download_bq():
    table_name = request.args.get("table")
    if not table_name:
        return "Missing ?table parameter.", 400

    try:
        ensure_dataset_exists(BIGQUERY_DATASET)
    except Exception as e:
        return f"Dataset error: {str(e)}", 500

    query = f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis`"
    try:
        results = bq_client.query(query).result()
    except Exception as e:
        return f"BigQuery query error: {str(e)}", 500

    temp_path = f"/tmp/{table_name}_analysis.csv"
    with open(temp_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        headers = [field.name for field in results.schema]
        writer.writerow(headers)
        for row in results:
            writer.writerow(list(row.values()))

    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=f"{table_name}_analysis.csv")

if __name__ == '__main__':
    app.run(debug=True)



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))




