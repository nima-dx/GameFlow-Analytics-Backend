from airflow import DAG
from datetime import datetime
from airflow_dbt.operators.dbt_operator import DbtRunOperator
import os

DBT_DIR = os.environ.get("DBT_HOME")

if not DBT_DIR:
    raise ValueError("DBT_HOME environment variable is not set")

with DAG(
    dag_id="dbt_dag_1",
    description="creates most_leagues_in_sport dbt model in analytics dataset",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_run = DbtRunOperator(
        task_id="dbt_run_most_leagues",
        dir=DBT_DIR,
        select="most_leagues_in_sport",
        profiles_dir=DBT_DIR,
    )
