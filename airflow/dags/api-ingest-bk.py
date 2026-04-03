import json
from datetime import datetime
from pathlib import Path

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


def fetch_and_save_leagues():
    """Fetch leagues data from TheSportsDB API and save to JSON file."""
    api_key = 149076
    leagues_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/all_leagues.php"

    # Make the API request
    response = requests.get(leagues_url)
    response.raise_for_status()

    # Get the JSON data
    data = response.json()

    # Create the directory if it doesn't exist
    output_dir = Path("/app/airflow/data/api-ingest")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save to file
    output_file = output_dir / "leagues.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Successfully saved leagues data to {output_file}")
    print(f"Total leagues fetched: {len(data.get('leagues', []))}")


# Define the DAG
with DAG(
    dag_id="api_ingest_bk",
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
