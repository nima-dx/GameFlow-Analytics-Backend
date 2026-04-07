from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from ingestion.api_extractors import fetch_and_save_leagues


# Define the DAG
with DAG(
    dag_id="api_ingest",
    description="Fetch leagues data from TheSportsDB API and save to JSON file",
    start_date=datetime(2026,4,1),
    schedule="@daily",
    catchup=True
) as dag:

    fetch_leagues_task = PythonOperator(
        task_id="fetch_and_save_leagues",
        python_callable=fetch_and_save_leagues,
    )

    end_task = EmptyOperator(
        task_id="end",
        trigger_rule="one_success"
    )

    # tasks hierachy:
    fetch_leagues_task >> end_task
