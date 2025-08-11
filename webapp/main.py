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


def run_analysis(table_name):
    """Runs a fresh analysis query, saves results to GCS and BigQuery."""
    ensure_dataset_exists(BIGQUERY_DATASET)

    # Check if table exists
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

    # Upload to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(local_csv)

    # Save to permanent BigQuery table
    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_TRUNCATE"
    )
    bq_client.query(query, job_config=job_config).result()

    return table_name


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        if not uploaded_file:
            flash("No file uploaded.", "error")
            return redirect(url_for('index'))

        file_path = f"/tmp/{uploaded_file.filename}"
        uploaded_file.save(file_path)

        # Upload to GCS
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(uploaded_file.filename)
            blob.upload_from_filename(file_path)
        except Exception as e:
            flash(f"Failed to upload to GCS: {e}", "error")
            return redirect(url_for('index'))

        # If SQL file, trigger import
        if uploaded_file.filename.endswith('.sql'):
            try:
                message_data = {'name': uploaded_file.filename, 'bucket': BUCKET_NAME}
                publisher.publish(topic_path, data=json.dumps(message_data).encode('utf-8'))
            except Exception as e:
                flash(f"Failed to publish SQL import message: {e}", "error")
                return redirect(url_for('index'))

        table_name = os.path.splitext(uploaded_file.filename)[0]

        try:
            run_analysis(table_name)
            flash(f"Uploaded {uploaded_file.filename} and analysis complete.", "success")
        except ValueError as e:
            flash(str(e), "error")
        except Exception as e:
            flash(f"Unexpected error: {e}", "error")

        return redirect(url_for('index'))

    return render_template('index.html')


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




