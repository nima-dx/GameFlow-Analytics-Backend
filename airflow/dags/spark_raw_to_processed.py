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
        gs://gameflow-ingestion-raw/event-stats*.json \
        gs://gameflow-ingestion-processed/event_stats
    """,
    env={
        "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
        "PYSPARK_PYTHON": "/usr/local/bin/python3",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
    },
    append_env=True,
)
    transform_event_timeline = BashOperator(
        task_id="transform_event_timeline",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/event_timeline.py \
        gs://gameflow-ingestion-raw/event_timeline*.json \
        gs://gameflow-ingestion-processed/event_timeline
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
    transform_events = BashOperator(
        task_id="transform_events",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/event_job.py \
        gs://gameflow-ingestion-raw/events-*.json \
        gs://gameflow-ingestion-processed/events
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
    transform_teams = BashOperator(
        task_id="transform_teams",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/teams_job.py \
        gs://gameflow-ingestion-raw/teams-*.json \
        gs://gameflow-ingestion-processed/teams
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
    transform_players = BashOperator(
        task_id="transform_players",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/players_job.py \
        gs://gameflow-ingestion-raw/players-*.json \
        gs://gameflow-ingestion-processed/players
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
    transform_seasons = BashOperator(
        task_id="transform_seasons",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/seasons_job.py \
        gs://gameflow-ingestion-raw/seasons-*.json \
        gs://gameflow-ingestion-processed/seasons
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
    transform_leagues_job = BashOperator(
        task_id="transform_leagues_job",
        bash_command=f"""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --conf spark.pyspark.python=/usr/local/bin/python3 \
        --conf spark.pyspark.driver.python=/usr/local/bin/python3 \
        --jars /app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.driver.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.executor.extraClassPath=/app/jars/gcs-connector-hadoop3-2.2.5-shaded.jar \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /app/spark_jobs/src/leagues_job.py \
        gs://gameflow-ingestion-raw/leagues*.json \
        gs://gameflow-ingestion-processed/leagues
        """,
        env={
            "GOOGLE_APPLICATION_CREDENTIALS": "/app/gcp-credentials.json",
            "PYSPARK_PYTHON": "/usr/local/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3",
        },
        append_env=True,
    )
