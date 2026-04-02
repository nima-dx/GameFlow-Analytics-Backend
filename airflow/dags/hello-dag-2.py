import json
from datetime import datetime
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    "second-dag",
    start_date=datetime(2026,3, 4),
    # catch_up=True,
    schedule="@daily"
)as dag:

    wait_for_hello_task = ExternalTaskSensor(
        task_id="task_sensor",
        external_dag_id="first-dag",
        external_task_id="first-task",
        allowed_states=["success"]
    )
    world_task = BashOperator(
        task_id = "world-task",
        bash_command = "echo world!"
    )

wait_for_hello_task >> world_task
