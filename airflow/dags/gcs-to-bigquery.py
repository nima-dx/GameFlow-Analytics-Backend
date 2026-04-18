from datetime import datetime
import json
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = "le-wagon-data-atelier"
DATASET_ID = "raw_dataset"
BUCKET_NAME = "gameflow-ingestion-raw"
LOCATION = "US"

SOURCE_OBJECT = "f1_calendar_2026.json"
NDJSON_OBJECT = "tmp/f1_calendar_2026.ndjson"
DESTINATION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.test_raw_f1_calendar_2026"

import json
import tempfile
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# ── Config ─────────────────────────────────────────────────────────────────────


SEASONS     = ["2026", "2025", "2024"]
FILE_TYPES  = ["f1_calendar", "f1_race_results"]


# ── Helper ─────────────────────────────────────────────────────────────────────
def convert_json_array_to_ndjson(source_object, ndjson_object, **kwargs):
    gcs = GCSHook(gcp_conn_id="google_cloud_connection")

    with tempfile.NamedTemporaryFile(mode="w+b", suffix=".json", delete=True) as src_tmp, \
         tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=True, encoding="utf-8") as ndjson_tmp:

        gcs.download(
            bucket_name=BUCKET_NAME,
            object_name=source_object,
            filename=src_tmp.name,
        )

        with open(src_tmp.name, "r", encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, list):
            raise ValueError(f"Expected top-level JSON array in {source_object}.")

        for row in data:
            ndjson_tmp.write(json.dumps(row, ensure_ascii=False) + "\n")

        ndjson_tmp.flush()

        gcs.upload(
            bucket_name=BUCKET_NAME,
            object_name=ndjson_object,
            filename=ndjson_tmp.name,
            mime_type="application/x-ndjson",
        )

        print(f"Uploaded NDJSON to gs://{BUCKET_NAME}/{ndjson_object}")


# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="load_f1_data_to_bq",
    description="Convert F1 JSON files to NDJSON and load to BigQuery",
    start_date=datetime(2026, 4, 16),
    schedule=None,
    catchup=False,
    tags=["gcs", "bigquery", "json", "f1"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    previous = start

    for season in SEASONS:
        for file_type in FILE_TYPES:
            source_object     = f"{file_type}_{season}.json"
            ndjson_object     = f"tmp/{file_type}_{season}.ndjson"
            destination_table = f"{PROJECT_ID}.{DATASET_ID}.{file_type}_{season}"
            task_suffix       = f"{file_type}_{season}"

            convert_task = PythonOperator(
                task_id=f"convert_{task_suffix}",
                python_callable=convert_json_array_to_ndjson,
                op_kwargs={
                    "source_object": source_object,
                    "ndjson_object": ndjson_object,
                },
            )

            load_task = GCSToBigQueryOperator(
                task_id=f"load_{task_suffix}",
                bucket=BUCKET_NAME,
                source_objects=[ndjson_object],
                destination_project_dataset_table=destination_table,
                source_format="NEWLINE_DELIMITED_JSON",
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                autodetect=True,
                gcp_conn_id="google_cloud_connection",
                location=LOCATION,
            )

            previous >> convert_task >> load_task
            previous = load_task

    previous >> end
