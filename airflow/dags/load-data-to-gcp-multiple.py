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

FILES = [
    {
        "source": "f1_calendar_2026.json",
        "ndjson": "tmp/f1_calendar_2026.ndjson",
        "table": f"{PROJECT_ID}.{DATASET_ID}.raw_f1_calendar_2026",
        "task_id": "f1_calendar",
    },
    {
        "source": "f1_race_results_2026.json",
        "ndjson": "tmp/f1_race_results_2026.ndjson",
        "table": f"{PROJECT_ID}.{DATASET_ID}.raw_f1_race_results_2026",
        "task_id": "f1_race_results",
    },
]


def make_convert_fn(source_object, ndjson_object):
    def convert_json_array_to_ndjson(**kwargs):
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
                raise ValueError("Expected top-level JSON array.")

            for row in data:
                ndjson_tmp.write(json.dumps(row, ensure_ascii=False) + "\n")

            ndjson_tmp.flush()

            gcs.upload(
                bucket_name=BUCKET_NAME,
                object_name=ndjson_object,
                filename=ndjson_tmp.name,
                mime_type="application/x-ndjson",
            )

    return convert_json_array_to_ndjson


with DAG(
    dag_id="load_f1_data_to_bq",
    start_date=datetime(2026, 4, 16),
    schedule=None,
    catchup=False,
    tags=["gcs", "bigquery", "json", "f1"],
) as dag:

    for file in FILES:
        convert = PythonOperator(
            task_id=f"convert_{file['task_id']}_to_ndjson",
            python_callable=make_convert_fn(file["source"], file["ndjson"]),
        )

        load = GCSToBigQueryOperator(
            task_id=f"load_{file['task_id']}_to_bq",
            bucket=BUCKET_NAME,
            source_objects=[file["ndjson"]],
            destination_project_dataset_table=file["table"],
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            autodetect=True,
            gcp_conn_id="google_cloud_connection",
            location=LOCATION,
        )

        convert >> load
