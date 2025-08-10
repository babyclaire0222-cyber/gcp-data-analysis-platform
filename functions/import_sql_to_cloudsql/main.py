import os
import json
import base64
import subprocess

PROJECT_ID = os.environ.get("GCP_PROJECT", "data-analysis-webapp")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "data-analysis-upload-1000")
INSTANCE_NAME = os.environ.get("CLOUDSQL_INSTANCE", "data-analysis-db")
DATABASE_NAME = os.environ.get("CLOUDSQL_DATABASE", "data_analysis")
REGION = os.environ.get("REGION", "asia-southeast1")

def import_sql(event, context):
    pubsub_message = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    file_name = pubsub_message.get("name")
    bucket_name = pubsub_message.get("bucket")

    if not file_name.endswith(".sql"):
        print(f"Skipping non-SQL file: {file_name}")
        return

    gcs_path = f"gs://{bucket_name}/{file_name}"
    try:
        subprocess.run(
            [
                "gcloud", "sql", "import", "sql", INSTANCE_NAME, gcs_path,
                "--project", PROJECT_ID,
                "--database", DATABASE_NAME,
                f"--region={REGION}"
            ],
            check=True
        )
        print(f"Import completed for {file_name}")
    except subprocess.CalledProcessError as e:
        print(f"Error importing {file_name}: {e}")
