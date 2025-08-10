import os
import json
from google.cloud import bigquery, pubsub_v1

PROJECT_ID = os.environ.get('GCP_PROJECT', 'data-analysis-webapp')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'analysis_dataset')
PUBSUB_TOPIC_FOR_SQL_IMPORT = os.environ.get('PUBSUB_TOPIC_FOR_SQL_IMPORT', 'sql-import-topic')

bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC_FOR_SQL_IMPORT)

def process_upload(event, context):
    file_name = event['name']
    bucket_name = event['bucket']

    if file_name.endswith('.csv') or file_name.endswith('.json'):
        table_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}.{file_name.replace('.', '_')}"
        uri = f"gs://{bucket_name}/{file_name}"
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV if file_name.endswith('.csv') else bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()
        print(f"Loaded {file_name} into {table_id}")

    elif file_name.endswith('.sql'):
        message_data = {'name': file_name, 'bucket': bucket_name}
        publisher.publish(topic_path, data=json.dumps(message_data).encode('utf-8'))
