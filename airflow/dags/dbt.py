from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

# Resolves to project root regardless of who runs it
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_gameflow"



print(ENV_FILE)
# print(PROJECT_ROOT)

with DAG(
    dag_id="dbt_gameflow",
    description="creates most_leagues_in_sport dbt model in analytics dataset",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # print(os.environ.get("GCP_PROJECT_ID"))
    # print(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
    dbt_run = BashOperator(
        task_id="dbt_run_most_leagues",
        bash_command="source /app/.env && dbt run --select most_leagues_in_sport --project-dir /app/dbt_gameflow --profiles-dir /app/dbt_gameflow"

    )

    dbt_run
