from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path

# Resolves to project root regardless of who runs it
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_gameflow"

with DAG(
    dag_id="dbt_gameflow",
    description="creates most_leagues_in_sport dbt model in analytics dataset",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    print('random')
    dbt_run = BashOperator(
        task_id="dbt_run_most_leagues",
        bash_command=f"source {ENV_FILE} && dbt run --select most_leagues_in_sport "
                     f"--project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run
