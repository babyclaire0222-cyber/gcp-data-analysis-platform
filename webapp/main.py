from flask import Flask, request, render_template, send_file, jsonify, g
import os
import json
import csv
import pandas as pd
from functools import wraps
from google.cloud import storage, bigquery
from google.cloud import pubsub_v1
import re
import io
from google.api_core.exceptions import NotFound

app = Flask(__name__)
app.secret_key = "supersecret"  # Needed for flash messages

# ===============================
# 🔹 IAP-only auth helpers
# ===============================
@app.before_request
def read_iap_identity():
    """
    When traffic comes through IAP, Google injects:
      X-Goog-Authenticated-User-Email: "accounts.google.com:<email>"
    """
    raw = request.headers.get("X-Goog-Authenticated-User-Email", "") or ""
    g.user_email = raw.split(":", 1)[1] if raw.startswith("accounts.google.com:") else None

def current_user_email():
    """Return the IAP-authenticated email, or None."""
    return getattr(g, "user_email", None)

def require_user(fn):
    """Decorator that ensures the request passed IAP."""
    @wraps(fn)
    def _wrap(*args, **kwargs):
        email = current_user_email()
        if not email:
            return jsonify({"success": False, "error": "Unauthorized"}), 401
        return fn(*args, **kwargs)
    return _wrap

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
# 🔹 Looker Studio publishing helpers (views in BigQuery)
# ===============================

def _create_or_replace_view(view_id: str, sql: str):
    """
    Create or replace a standard BigQuery view.
    view_id must be like: PROJECT.DATASET.VIEW_NAME
    """
    try:
        # If it exists, update the query
        existing = bq_client.get_table(view_id)
        existing.view_query = sql
        bq_client.update_table(existing, ["view_query"])
        return existing
    except NotFound:
        # Create fresh view
        table = bigquery.Table(view_id)
        table.view_query = sql
        return bq_client.create_table(table)

def publish_looker_views_for_table(table_name: str) -> dict:
    """
    For each REPORT in REPORTS, create a view named:
      <table>__<report_id>_v
    e.g. finance_data__dept_totals_v
    Returns a dict of {report_id: fully_qualified_view_id}
    """
    ensure_dataset_exists(BIGQUERY_DATASET)
    if not VALID_TABLE_RE.match(table_name):
        raise ValueError("Invalid table name. Use letters, numbers, or underscore only.")

    table_fq = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"
    created = {}
    for rid, meta in REPORTS.items():
        view_name = f"{table_name}__{rid}_v"
        view_fq = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{view_name}"
        sql = meta["sql"].replace("{table_fq}", table_fq)

        # Views must be a SELECT statement by itself (ours are)
        _create_or_replace_view(view_fq, sql)
        created[rid] = view_fq

    return created

# -------------------------------
# 📊 Prebuilt Finance Reports
# -------------------------------
REPORTS = {
    "dept_totals": {
        "label": "Total spend per department (6 months)",
        "sql": """
        SELECT department, SUM(amount) AS total_spent
        FROM `{table_fq}`
        GROUP BY department
        ORDER BY total_spent DESC
        """
    },
    "monthly_trend": {
        "label": "Monthly spend trend",
        "sql": """
        SELECT FORMAT_DATE('%Y-%m', DATE(date)) AS month,
               SUM(amount) AS total_spent
        FROM `{table_fq}`
        GROUP BY month
        ORDER BY month
        """
    },
    "top_expense_types": {
        "label": "Top 5 expense categories",
        "sql": """
        SELECT expense_type, SUM(amount) AS total_spent
        FROM `{table_fq}`
        GROUP BY expense_type
        ORDER BY total_spent DESC
        LIMIT 5
        """
    },
    "dept_month_matrix": {
        "label": "Department spend by month",
        "sql": """
        SELECT FORMAT_DATE('%Y-%m', DATE(date)) AS month,
               department,
               SUM(amount) AS total_spent
        FROM `{table_fq}`
        GROUP BY month, department
        ORDER BY month, department
        """
    },
    "avg_monthly_by_dept": {
        "label": "Average monthly spend per department",
        "sql": """
        WITH monthly AS (
          SELECT department,
                 FORMAT_DATE('%Y-%m', DATE(date)) AS month,
                 SUM(amount) AS monthly_spent
          FROM `{table_fq}`
          GROUP BY department, month
        )
        SELECT department, AVG(monthly_spent) AS avg_monthly_spent
        FROM monthly
        GROUP BY department
        ORDER BY avg_monthly_spent DESC
        """
    },
}

VALID_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")

def _fq_table(table_name: str) -> str:
    """Return fully-qualified table id, after validating a safe table name."""
    if not VALID_TABLE_RE.match(table_name):
        raise ValueError("Invalid table name. Use letters, numbers, or underscore only.")
    return f"{PROJECT_ID}.{BIGQUERY_DATASET}.{table_name}"

def _run_sql(sql: str, max_rows: int = 1000):
    job = bq_client.query(sql)
    result = job.result(max_results=max_rows)
    columns = [f.name for f in result.schema]
    rows = [list(row.values()) for row in result]
    return columns, rows

# ===============================
# 🔹 Routes (IAP-protected)
# ===============================
@app.route('/', methods=['GET', 'POST'])
@require_user
def index():
    user_email = current_user_email()

    if request.method == 'POST':
        uploaded_file = request.files.get('file')
        if uploaded_file:
            file_ext = os.path.splitext(uploaded_file.filename)[1].lower()
            file_path = f"/tmp/{uploaded_file.filename}"
            uploaded_file.save(file_path)

            table_name = os.path.splitext(uploaded_file.filename)[0].replace(" ", "_").lower()

            try:
                if file_ext == '.sql':
                    # Upload raw SQL to GCS then notify Pub/Sub
                    bucket = storage_client.bucket(BUCKET_NAME)
                    blob = bucket.blob(f"uploads/{uploaded_file.filename}")
                    blob.upload_from_filename(file_path)

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
                    "user": user_email
                })

            except Exception as e:
                return jsonify({"success": False, "error": str(e)}), 500

    return render_template('index.html')

@app.route('/download/<filename>')
@require_user
def download_file(filename):
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"analysis_results/{filename}")

    if not blob.exists():
        return f"File {filename} not found in analysis_results folder.", 404

    temp_path = f"/tmp/{filename}"
    blob.download_to_filename(temp_path)
    return send_file(temp_path, mimetype='text/csv', as_attachment=True, download_name=filename)

@app.route('/download_bq')
@require_user
def download_bq():
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
@require_user
def whoami():
    """Return the IAP-authenticated user's email."""
    return jsonify({"success": True, "email": current_user_email()})

# (optional) simple health endpoint without auth (keep if you want LB checks)
@app.route("/healthz")
def healthz():
    return "ok", 200

@app.route("/reports")
@require_user
def list_reports():
    """Return the list of available report ids + labels."""
    items = [{"id": k, "label": v["label"]} for k, v in REPORTS.items()]
    return jsonify({"reports": items})

@app.route("/run_report", methods=["POST"])
@require_user
def run_report():
    """
    Body JSON: { "report": "<id from /reports>", "table": "<your_table>", "limit": 1000? }
    Returns: { columns: [...], rows: [[...]], row_count: N }
    """
    data = request.get_json(silent=True) or {}
    report_id = data.get("report")
    table = data.get("table")
    limit = int(data.get("limit") or 1000)

    if report_id not in REPORTS:
        return jsonify({"success": False, "error": "Unknown report id."}), 400
    if not table:
        return jsonify({"success": False, "error": "Missing 'table'."}), 400

    try:
        table_fq = _fq_table(table)
        sql = REPORTS[report_id]["sql"].replace("{table_fq}", table_fq)
        columns, rows = _run_sql(sql, max_rows=limit)
        return jsonify({
            "success": True,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/download_report")
@require_user
def download_report():
    """
    Query params: ?report=<id>&table=<table>
    Streams a CSV file for the selected report.
    """
    report_id = request.args.get("report")
    table = request.args.get("table")
    if report_id not in REPORTS or not table:
        return "Missing or invalid parameters.", 400

    try:
        table_fq = _fq_table(table)
        sql = REPORTS[report_id]["sql"].replace("{table_fq}", table_fq)
        # No row limit for CSV; adjust if needed
        job = bq_client.query(sql)
        result = job.result()

        out = io.StringIO()
        writer = csv.writer(out)
        columns = [f.name for f in result.schema]
        writer.writerow(columns)
        for row in result:
            writer.writerow(list(row.values()))
        out.seek(0)

        # Send as file
        return send_file(
            io.BytesIO(out.read().encode("utf-8")),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{report_id}_{table}.csv"
        )
    except Exception as e:
        return f"Error: {e}", 500

@app.route("/publish_looker_views", methods=["POST"])
@require_user
def publish_looker_views():
    """
    Body JSON: { "table": "finance_data" }
    Creates/updates one view per prebuilt report, e.g.:
      data-analysis-webapp.analysis_dataset.finance_data__dept_totals_v
    Returns JSON listing the view IDs you can pick in Looker Studio.
    """
    data = request.get_json(silent=True) or {}
    table = data.get("table")
    if not table:
        return jsonify({"success": False, "error": "Missing 'table'."}), 400
    try:
        views = publish_looker_views_for_table(table)
        return jsonify({"success": True, "views": views})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/looker_help")
@require_user
def looker_help():
    """
    Returns the project/dataset and a short how-to for Looker Studio.
    """
    return jsonify({
        "success": True,
        "project_id": PROJECT_ID,
        "dataset": BIGQUERY_DATASET,
        "how_to": [
            "Open https://lookerstudio.google.com → Create → Report.",
            "Add data → BigQuery connector.",
            f"Pick project '{PROJECT_ID}' → dataset '{BIGQUERY_DATASET}'.",
            "Choose any of the *_v views you created (e.g. finance_data__dept_totals_v).",
            "Click CONNECT, then add charts (bar/line/pie) as needed."
        ],
        "tip": "Re-run /publish_looker_views after uploading a new base table name."
    })

# ===============================
# 🔹 Start Flask App
# ===============================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))





