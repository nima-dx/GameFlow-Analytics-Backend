# from datetime import datetime
# import json
# import tempfile

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.google.cloud.hooks.gcs import GCSHook
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# PROJECT_ID = "le-wagon-data-atelier"
# DATASET_ID = "raw_dataset"
# BUCKET_NAME = "gameflow-ingestion-raw"
# LOCATION = "US"

# SOURCE_OBJECT = "f1_calendar_2026.json"
# NDJSON_OBJECT = "tmp/f1_calendar_2026.ndjson"
# DESTINATION_TABLE = f"{PROJECT_ID}.{DATASET_ID}.test2_raw_f1_calendar_2026"


# def convert_json_array_to_ndjson(**kwargs):
#     gcs = GCSHook(gcp_conn_id="google_cloud_connection")

#     with tempfile.NamedTemporaryFile(mode="w+b", suffix=".json", delete=True) as src_tmp, \
#          tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=True, encoding="utf-8") as ndjson_tmp:

#         gcs.download(
#             bucket_name=BUCKET_NAME,
#             object_name=SOURCE_OBJECT,
#             filename=src_tmp.name,
#         )

#         with open(src_tmp.name, "r", encoding="utf-8") as f:
#             data = json.load(f)

#         if not isinstance(data, list):
#             raise ValueError("Expected top-level JSON array.")

#         for row in data:
#             ndjson_tmp.write(json.dumps(row, ensure_ascii=False) + "\n")

#         ndjson_tmp.flush()

#         gcs.upload(
#             bucket_name=BUCKET_NAME,
#             object_name=NDJSON_OBJECT,
#             filename=ndjson_tmp.name,
#             mime_type="application/x-ndjson",
#         )


# with DAG(
#     dag_id="load_f1_calendar_to_bq",
#     start_date=datetime(2026, 4, 16),
#     schedule=None,
#     catchup=False,
#     tags=["gcs", "bigquery", "json", "f1"],
# ) as dag:

#     convert_to_ndjson = PythonOperator(
#         task_id="convert_json_array_to_ndjson",
#         python_callable=convert_json_array_to_ndjson,
#     )

#     load_to_bigquery = GCSToBigQueryOperator(
#         task_id="load_raw_f1_calendar_2026",
#         bucket=BUCKET_NAME,
#         source_objects=[NDJSON_OBJECT],
#         destination_project_dataset_table=DESTINATION_TABLE,
#         source_format="NEWLINE_DELIMITED_JSON",
#         write_disposition="WRITE_TRUNCATE",
#         create_disposition="CREATE_IF_NEEDED",
#         autodetect=True,
#         gcp_conn_id="google_cloud_connection",
#         location=LOCATION,
#     )

#     convert_to_ndjson >> load_to_bigquery

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


def make_dag(dag_id, source_object, ndjson_object, destination_table, tags):
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2026, 4, 16),
        schedule=None,
        catchup=False,
        tags=tags,
    ) as dag:

        convert_to_ndjson = PythonOperator(
            task_id="convert_json_array_to_ndjson",
            python_callable=make_convert_fn(source_object, ndjson_object),
        )

        load_to_bigquery = GCSToBigQueryOperator(
            task_id=f"load_{dag_id}",
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

        convert_to_ndjson >> load_to_bigquery

    return dag


dag_f1_calendar = make_dag(
    dag_id="load_f1_calendar_to_bq",
    source_object="f1_calendar_2026.json",
    ndjson_object="tmp/f1_calendar_2026.ndjson",
    destination_table=f"{PROJECT_ID}.{DATASET_ID}.f1_calendar",
    tags=["gcs", "bigquery", "json", "f1"],
)

dag_f1_results = make_dag(
    dag_id="load_f1_race_results_to_bq",
    source_object="f1_race_results_2026.json",
    ndjson_object="tmp/f1_race_results_2026.ndjson",
    destination_table=f"{PROJECT_ID}.{DATASET_ID}.race_results",
    tags=["gcs", "bigquery", "json", "f1"],
)
