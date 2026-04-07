from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_raw_to_processed",
    description="Read leagues JSON from raw GCS bucket and write parquet to processed GCS bucket",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["gameflow", "spark", "gcs"],
) as dag:

     transform_leagues = SparkSubmitOperator(
        task_id="transform_leagues",
        conn_id="spark_default",
        application="/app/spark_jobs/src/move_and_transform.py",
        jars="/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
        conf={
            "spark.driver.extraClassPath": "/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
            "spark.executor.extraClassPath": "/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        },
        env_vars={
            "RAW_PATH": "gs://gameflow-ingestion-raw/leagues.json",
            "PROCESSED_PATH": "gs://gameflow-ingestion-processed/leagues_parquet",
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
        },
        verbose=True,
     )
