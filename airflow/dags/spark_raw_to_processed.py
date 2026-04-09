from datetime import datetime

from airflow import DAG
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="spark_raw_to_processed",
    description="Read leagues JSON from raw GCS bucket and write parquet to processed GCS bucket",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["gameflow", "spark", "gcs"],
) as dag:

    #  transform_leagues = BashOperator(
    #     task_id="transform_leagues",
    #     conn_id="spark_default",
    #     application="/app/spark_jobs/src/move_and_transform.py",
    #     spark_binary="/opt/spark/bin/spark-submit",
    #     jars="/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
    #     conf={
    #         "spark.master": "spark://spark:7077",
    #         "spark.driver.extraClassPath": "/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
    #         "spark.executor.extraClassPath": "/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar",
    #         "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    #         "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    #     },
    #     application_args=[
    #         "gs://gameflow-ingestion-raw/leagues.json",
    #         "gs://gameflow-ingestion-processed/leagues_parquet",
    #     ],
    #    # env_vars={
    #    #     "RAW_PATH": "gs://gameflow-ingestion-raw/leagues.json",
    #    #     "PROCESSED_PATH": "gs://gameflow-ingestion-processed/leagues_parquet",
    #    #     "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
    #    # },
    #     verbose=True,
    #  )
    transform_leagues = BashOperator(
    task_id="transform_leagues",
    bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/move_and_transform.py \
        gs://gameflow-ingestion-raw/leagues.json \
        gs://gameflow-ingestion-processed/leagues_parquet
    """,
    env={
        "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
        "PYSPARK_PYTHON": "/usr/local/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
    },
    append_env=True,
)
