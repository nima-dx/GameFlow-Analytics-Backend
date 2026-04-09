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

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
DATA_DIR = f"{AIRFLOW_HOME}/data/api-ingest"
LOAD_LOG_FILE = f"{AIRFLOW_HOME}/data/load_log.json"


def load_log_read():
    """Read the load log JSON file and return list of loaded files."""
    # Create the log file if it doesn't exist
    if not os.path.exists(LOAD_LOG_FILE):
        with open(LOAD_LOG_FILE, "w") as f:
            json.dump([], f)

    # Read and return the list of loaded files
    with open(LOAD_LOG_FILE, "r") as f:
        load_log = json.load(f)

    # Extract file names from the log
    return [entry.get("strFile") for entry in load_log]


def log_load_write(file_name):
    """Add a file name to the load log after successful upload."""
    # Read existing log
    with open(LOAD_LOG_FILE, "r") as f:
        load_log = json.load(f)

    # Add new entry if it doesn't exist
    if not any(entry.get("strFile") == file_name for entry in load_log):
        load_log.append({"strFile": file_name})

    # Write back to file
    with open(LOAD_LOG_FILE, "w") as f:
        json.dump(load_log, f, indent=2)


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

    # Get list of already loaded files
    loaded_files = load_log_read()

    # Create a LocalFilesystemToGCSOperator task for each JSON file
    for file_path in json_files:
        file_name = os.path.basename(file_path)

        # Skip if file has already been loaded
        if file_name in loaded_files:
            print(f"Skipping {file_name} - already loaded")
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

        # Create a task to log the file after successful upload
        log_task = PythonOperator(
            task_id=f"log_{file_name.replace('.', '_').replace('-', '_')}",
            python_callable=log_load_write,
            op_kwargs={"file_name": file_name}
        )

        upload_tasks.append(upload_task)
        wait_for_transform_task >> upload_task >> log_task
