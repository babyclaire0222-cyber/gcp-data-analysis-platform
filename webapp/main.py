from flask import Flask, request, render_template, send_file, redirect, url_for, flash, jsonify
import os
import json
import csv
import pandas as pd
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1
import firebase_admin
from firebase_admin import auth, credentials

app = Flask(__name__)
app.secret_key = "supersecret"  # Needed for flash messages

# ===============================
# 🔹 Firebase Authentication Init
# ===============================
if not firebase_admin._apps:
    cred = credentials.ApplicationDefault()  # Cloud Run / GCP SA
    firebase_admin.initialize_app(cred)

def _extract_id_token():
    """Get Firebase ID token from header or query string."""
    # 1) Header: Authorization: Bearer <token>
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        return auth_header.split("Bearer ", 1)[1].strip()

    # 2) Query string: ?auth=<token> or ?token=<token>
    token = request.args.get("auth") or request.args.get("token")
    if token:
        return token.strip()

    # 3) (Optional) Form field fallback for multipart
    token = request.form.get("auth") or request.form.get("token")
    if token:
        return token.strip()

    return None

def verify_firebase_token():
    """Verify Firebase ID token from header or query param."""
    raw_token = _extract_id_token()
    if not raw_token:
        return None
    try:
        decoded = auth.verify_id_token(raw_token)
        return decoded  # includes uid, email, etc.
    except Exception as e:
        print(f"Auth error: {e}")
        return None

from functools import wraps
def login_required(f):
    """Decorator to protect routes with Firebase auth (header or query)."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        user = verify_firebase_token()
        if not user:
            return jsonify({"success": False, "error": "Unauthorized. Please log in."}), 401
        return f(user, *args, **kwargs)
    return wrapper

# ===============================
# 🔹 GCP Configuration
# ===============================
PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'data-analysis-upload-1000')
PUBSUB_TOPIC_FOR_SQL_IMPORT = os.environ.get('PUBSUB_TOPIC_FOR_SQL_IMPORT', 'sql-import-topic')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')

# Clients
storage_client = storage.Client()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_FOR_SQL_IMPORT)

# ===============================
# 🔹 Helper functions
# ===============================
def ensure_dataset_exists(dataset_id):
    """Create dataset if it doesn't already exist."""
    dataset_ref = bq_client.dataset(dataset_id)
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "asia-southeast1"  # Match your region
        bq_client.create_dataset(dataset)

def load_to_bigquery(file_path, filename, table_name):
    """Loads supported file types into BigQuery with autodetect schema."""
    ensure_dataset_exists(BIGQUERY_DATASET)

    ext = os.path.splitext(filename)[1].lower()
    tmp_path = file_path

    # Convert Excel to CSV before loading
    if ext in [".xls", ".xlsx"]:
        df = pd.read_excel(file_path)
        tmp_path = f"/tmp/{table_name}.csv"
        df.to_csv(tmp_path, index=False)
        source_format = bigquery.SourceFormat.CSV
        skip_rows = 1
    elif ext == ".csv":
        source_format = bigquery.SourceFormat.CSV
        skip_rows = 1
    elif ext == ".json":
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        skip_rows = 0
    elif ext == ".parquet":
        source_format = bigquery.SourceFormat.PARQUET
        skip_rows = 0
    else:
        raise ValueError("Unsupported file type. Use CSV, Excel, JSON, or Parquet.")

    # Upload to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"uploads/{filename}")
    blob.upload_from_filename(tmp_path)

    # Load into BigQuery
    table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        source_format=source_format,
        skip_leading_rows=skip_rows,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(tmp_path, "rb") as source_file:
        load_job = bq_client.load_table_from_file(source_file, table_id, job_config=job_config)

    load_job.result()

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

    # Upload to GCS
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{table_name}_results.csv")
    blob.upload_from_filename(local_csv)

    # Save to BigQuery _analysis table
    destination_table = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}_analysis"
    job_config = bigquery.QueryJobConfig(
        destination=destination_table,
        write_disposition="WRITE_TRUNCATE"
    )
    bq_client.query(query, job_config=job_config).result()

    return table_name

# ===============================
# 🔹 Routes
# ===============================
@app.route('/', methods=['GET', 'POST'])
def index():
    user = verify_firebase_token(request)
    if not user:
        return jsonify({"success": False, "error": "Unauthorized. Please log in."}), 401

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        if uploaded_file:
            file_ext = os.path.splitext(uploaded_file.filename)[1].lower()
            file_path = f"/tmp/{uploaded_file.filename}"
            uploaded_file.save(file_path)

            table_name = os.path.splitext(uploaded_file.filename)[0].replace(" ", "_").lower()

            try:
                if file_ext == '.sql':
                    # Handle SQL files with Pub/Sub
                    message_data = {'name': uploaded_file.filename, 'bucket': BUCKET_NAME}
                    publisher.publish(topic_path, data=json.dumps(message_data).encode('utf-8'))

                elif file_ext in ['.xlsx', '.xls']:
                    # Convert Excel to CSV first
                    csv_path = f"/tmp/{table_name}.csv"
                    df = pd.read_excel(file_path)
                    df.to_csv(csv_path, index=False)
                    load_to_bigquery(csv_path, f"{table_name}.csv", table_name)

                elif file_ext in ['.csv', '.json', '.parquet']:
                    load_to_bigquery(file_path, uploaded_file.filename, table_name)

                else:
                    return jsonify({"success": False, "error": "Unsupported file format."}), 400

                # Run downstream analysis
                run_analysis(table_name)
                return jsonify({
                    "success": True,
                    "message": f"✅ Uploaded {uploaded_file.filename} and analysis complete",
                    "table": table_name,
                    "user": user.get("email")
                })

            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500

    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    user = verify_firebase_token(request)
    if not user:
        return "Unauthorized", 401

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{filename}")

    if not blob.exists():
        return f"File {filename} not found in analysis_results folder.", 404

    temp_path = f"/tmp/{filename}"
    blob.download_to_filename(temp_path)
    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=filename)

@app.route('/download_bq')
def download_bq():
    user = verify_firebase_token(request)
    if not user:
        return "Unauthorized", 401

    table_name = request.args.get("table")
    if not table_name:
        return "Missing ?table parameter.", 400

    try:
        run_analysis(table_name)
    except ValueError as e:
        return str(e), 400

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

@app.route("/whoami")
def whoami():
    """Return basic info about the logged-in Firebase user."""
    user = verify_firebase_token(request)
    if not user:
        return jsonify({"success": False, "error": "Unauthorized"}), 401

    return jsonify({
        "success": True,
        "uid": user.get("uid"),
        "email": user.get("email"),
        "name": user.get("name")
    })

# ===============================
# 🔹 Start Flask App
# ===============================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))




