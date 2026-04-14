import os
import json
import glob
from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIR = f"{AIRFLOW_HOME}/data/api-ingest"
LOAD_LOG_FILE = f"{AIRFLOW_HOME}/data/load_log.json"


def process_files(file_list, **context):
    """Process the list of files from GCS and save to JSON."""
    with open(LOAD_LOG_FILE, "w") as f:
        json.dump(file_list, f, indent=2)
    print(f"Saved {len(file_list)} files to {LOAD_LOG_FILE}")
    return file_list


#  all JSON files in the local data directory.
json_files = glob.glob(f"{DATA_DIR}/*.json") if os.path.exists(DATA_DIR) else []

with DAG(
    "load_multiple",
    start_date=datetime(2026,4,6),
    schedule="@daily",
    catchup=False,
) as dag:

    # GCP Configuration
    BUCKET_NAME = "gameflow-ingestion-raw"

    wait_for_transform_task = ExternalTaskSensor(
        task_id="ingest_sensor",
        external_dag_id="api_ingest",
        external_task_id="end",
        allowed_states=["success"],
        execution_date_fn=lambda dt: dt.replace(hour=0, minute=0, second=0, microsecond=0),
    )

    # List all files in GCS bucket
    list_gcs_files = GCSListObjectsOperator(
        task_id='list_gcs_files',
        bucket=BUCKET_NAME,
        delimiter='/',
        gcp_conn_id="google_cloud_connection"
    )

    # Process and save the list of files to JSON
    process_task = PythonOperator(
        task_id='process_files',
        python_callable=process_files,
        op_args=['{{ task_instance.xcom_pull(task_ids="list_gcs_files") }}'],
    )

    upload_tasks = []

    # Helper function to check if file already exists in GCS
    def file_exists_in_gcs(file_name):
        """Check if file exists in the saved load log (GCS listing)."""
        if not os.path.exists(LOAD_LOG_FILE):
            return False
        try:
            with open(LOAD_LOG_FILE, "r") as f:
                loaded_files = json.load(f)
                return file_name in loaded_files
        except:
            return False

    # Create a LocalFilesystemToGCSOperator task for each JSON file
    for file_path in json_files:
        file_name = os.path.basename(file_path)

        # Skip if file has already been loaded in GCS
        if file_exists_in_gcs(file_name):
            print(f"Skipping {file_name} - already loaded in GCS")
            continue

        # Create a valid task_id (replace dots and special chars with underscores)
        task_id = f"upload_{file_name.replace('.', '_').replace('-', '_')}"

        upload_task = LocalFilesystemToGCSOperator(
            task_id=task_id,
            src=file_path,
            dst=file_name,
            bucket=BUCKET_NAME,
            gcp_conn_id="google_cloud_connection"
        )

        upload_tasks.append(upload_task)
        process_task >> upload_task

    # Connect the chain: sensor -> list GCS files -> process files -> upload tasks
    # wait_for_transform_task >> list_gcs_files >> process_task >> upload_task
