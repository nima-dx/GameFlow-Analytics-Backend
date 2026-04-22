from airflow import DAG
from datetime import datetime
from airflow_dbt.operators.dbt_operator import DbtRunOperator
import os
from airflow.models.baseoperator import chain


DBT_DIR = os.environ.get("DBT_HOME")

if not DBT_DIR:
    raise ValueError("DBT_HOME environment variable is not set")

with DAG(
    dag_id="create_all_dbt_models_dag",
    description="run all the dbt models for sportflow",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    task_f1_calendar = DbtRunOperator(
        task_id="task_f1_calendar",
        dir=DBT_DIR,
        select="f1_calendar",
        profiles_dir=DBT_DIR,
    )

    task_f1_race_results = DbtRunOperator(
        task_id="task_f1_race_results",
        dir=DBT_DIR,
        select="f1_race_results",
        profiles_dir=DBT_DIR,
    )

    task_event_log = DbtRunOperator(
        task_id="task_event_log",
        dir=DBT_DIR,
        select="int_event_log",
        profiles_dir=DBT_DIR,
    )

    task_int_event_outlook = DbtRunOperator(
        task_id="task_int_event_outlook",
        dir=DBT_DIR,
        select="int_event_outlook",
        profiles_dir=DBT_DIR,
    )

    task_int_players_timeline = DbtRunOperator(
        task_id="task_int_players_timeline",
        dir=DBT_DIR,
        select="int_players_timeline",
        profiles_dir=DBT_DIR,
    )

    task_players_performance = DbtRunOperator(
        task_id="task_players_performance",
        dir=DBT_DIR,
        select="players_performance",
        profiles_dir=DBT_DIR,
    )

    task_team_performance_metrics = DbtRunOperator(
        task_id="task_team_performance_metrics",
        dir=DBT_DIR,
        select="team_performance_metrics",
        profiles_dir=DBT_DIR,
    )

    task_event_log >> \
    task_int_event_outlook >> \
    task_int_players_timeline >> \
    task_players_performance >> \
    task_team_performance_metrics >> \
    task_f1_calendar >> \
    task_f1_race_results
