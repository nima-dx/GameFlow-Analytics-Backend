import json
from pathlib import Path

import requests


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
