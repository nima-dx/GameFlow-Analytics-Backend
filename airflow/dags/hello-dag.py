import json
from datetime import datetime
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator



with DAG(
    "first-dag",
    start_date=datetime(2026,3, 4),
    catchup=True,
    schedule="@daily"
)as dag:

    first_task = BashOperator(
        task_id = "first-task",
        bash_command = "echo Hello"
    )
