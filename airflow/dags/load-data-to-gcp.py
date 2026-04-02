import os
from datetime import datetime

from airflow import DAG

from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

with DAG(
    "load",
    default_args={"depends_on_past": False},
    start_date=datetime(2021, 6, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    date = "{{ ds[:7] }}"
    data_file = f"{AIRFLOW_HOME}/data/api-ingest/leagues.json"

     # GCP Configuration
    PROJECT_ID = "le-wagon-data-atelier"
    BUCKET_NAME = "gameflow-ingestion-raw"
    # DATASET_NAME = "taxi_data"
    # TABLE_NAME = "yellow_tripdata"

    wait_for_transform_task = ExternalTaskSensor(
        task_id="transform_sensor",
        # $CODE_BEGIN
        external_dag_id="api_ingest",
        allowed_states=["success"],
        poke_interval=10,
        timeout=60 * 10,
        # $CODE_END
    )

    upload_local_file_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_local_file_to_gcs",
        # $CODE_BEGIN
        gcp_conn_id="google_cloud_connection",
        src=data_file,
        dst=f"leagues.json",
        bucket="gameflow-ingestion-raw",
        # $CODE_END
    )


    # Organise your tasks hierachy here
    # $CHA_BEGIN
    (
        wait_for_transform_task
        >> upload_local_file_to_gcs_task
    )
    # $CHA_END
