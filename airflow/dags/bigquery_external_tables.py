from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

SQL_FILE_PATH = "/app/bigquery/create_external_tables.sql"

with open(SQL_FILE_PATH, "r") as f:
    create_external_tables_sql = f.read()
with DAG(
    dag_id="bigquery_external_tables",
    description="Create or refresh BigQuery external tables over processed parquet files",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["bigquery", "gcs", "external_tables"],
) as dag:

    create_external_tables = BigQueryInsertJobOperator(
        task_id="create_external_tables",
        configuration={
            "query": {
                "query": create_external_tables_sql,
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_connection",
    )
