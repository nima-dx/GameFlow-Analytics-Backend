import os
import glob
from datetime import datetime

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIR = f"{AIRFLOW_HOME}/data/api-ingest"

# Get all JSON files at DAG parse time
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

    upload_tasks = []

    # Create a LocalFilesystemToGCSOperator task for each JSON file
    for file_path in json_files:
        file_name = os.path.basename(file_path)
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
        wait_for_transform_task >> upload_task
