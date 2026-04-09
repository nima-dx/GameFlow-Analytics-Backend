from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow_dbt.operators.dbt_operator import DbtRunOperator

# DBT_PROJECT_DIR = "/app/dbt_gameflow"
DBT_PROJECT_DIR = "/home/arthurdeetu/code/joaquin-ortega84/GameFlow-Analytics-Backend/dbt_gameflow"
DBT_PROFILES_DIR = "/home/arthurdeetu/code/joaquin-ortega84/GameFlow-Analytics-Backend/dbt_gameflow"

with DAG(
    dag_id="dbt_gameflow_new",
    description="creates most_leagues_in_sport dbt model in analytics dataset",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_run = DbtRunOperator(
        task_id="dbt_run_most_leagues",
        dir=DBT_PROJECT_DIR,
        select="most_leagues_in_sport",
        profiles_dir=DBT_PROFILES_DIR
    )

    dbt_run
