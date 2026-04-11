from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from ingestion.api_extractors import (
    fetch_and_save_leagues,
    extract_leagues_data,
    extract_teams_data,
    extract_players_data,
    extract_seasons_data,
    extract_league_all_events,
    extract_event_timeline_data,
    extract_event_stats_data,
    entity_caller
)


# Define the DAG
with DAG(
    dag_id="api_ingest",
    description="Fetch leagues data from TheSportsDB API and save to JSON file",
    start_date=datetime(2026,4,6),
    schedule="@daily",
    catchup=True
) as dag:

    fetch_leagues_task = PythonOperator(
        task_id="fetch_and_save_leagues",
        python_callable=fetch_and_save_leagues,
    )

    extract_leagues_task = PythonOperator(
        task_id="extract_leagues_data",
        python_callable=extract_leagues_data,
    )

    extract_teams_task = PythonOperator(
        task_id="extract_teams_data",
        python_callable=extract_teams_data,
    )

    extract_players_task = PythonOperator(
        task_id="extract_players_data",
        python_callable=extract_players_data,
    )

    extract_seasons_task = PythonOperator(
        task_id="extract_seasons_data",
        python_callable=extract_seasons_data,
    )

    extract_events_task = PythonOperator(
        task_id="extract_league_all_events",
        python_callable=extract_league_all_events,
    )

    extract_event_timeline_task = PythonOperator(
        task_id="extract_event_timeline_data",
        python_callable=extract_event_timeline_data,
    )

    extract_event_stats_task = PythonOperator(
        task_id="extract_event_stats_data",
        python_callable=extract_event_stats_data,
    )

    end_task = EmptyOperator(
        task_id="end",
        trigger_rule="one_success"
    )

    # tasks hierachy:
    extract_leagues_task >> extract_teams_task >> extract_players_task >> extract_seasons_task >> extract_events_task >> extract_event_timeline_task >> extract_event_stats_task >> end_task
