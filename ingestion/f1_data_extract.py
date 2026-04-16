import json
import os
from pathlib import Path

import pandas as pd
import requests
from google.cloud import storage

F1_LEAGUE_ID = 4370
F1_SEASON = "2026"
API_KEY = os.getenv("API_KEY", "149076")
GCS_BUCKET = "gameflow-ingestion-raw"
LOCAL_DIR = Path('/home/arthurdeetu/code/joaquin-ortega84/GameFlow-Analytics-Backend/joaquin/')


def upload_to_gcs(client, file_name, local_path):
    bucket = client.bucket(GCS_BUCKET)

    bucket.blob(file_name).upload_from_filename(str(local_path), content_type="application/json")
    print(f"Uploaded to gs://{GCS_BUCKET}/{file_name}")


def fetch_and_save_f1_calendar():
    url = (
        f"https://www.thesportsdb.com/api/v1/json/{API_KEY}/eventsseason.php"
        f"?id={F1_LEAGUE_ID}&s={F1_SEASON}"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    events = data.get("events") or []

    if not events:
        print("No events found.")
        return []

    races = [
        e for e in events
        if "grand prix" in e.get("strEvent", "").lower()
        and "qualifying" not in e.get("strEvent", "").lower()
        and "practice" not in e.get("strEvent", "").lower()
        and "sprint" not in e.get("strEvent", "").lower()
    ]

    if not races:
        print("No races found.")
        return []

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    file_name = f"f1_calendar_{F1_SEASON}.json"
    local_path = LOCAL_DIR / file_name

    with open(local_path, "w") as f:
        json.dump(races, f, indent=2)

    print(f"Saved calendar locally to {local_path} — {len(races)} races")
    return races


def fetch_and_save_f1_results(races):
    df = pd.DataFrame(races)
    completed = df[df['strStatus'] == "Match Finished"]

    if completed.empty:
        print("No completed races found.")
        return

    all_results = []

    for _, row in completed.iterrows():
        event_id = row['idEvent']
        url = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}/eventresults.php?id={event_id}"
        response = requests.get(url)
        results = response.json().get("results") or []
        all_results.extend(results)
        print(f"Fetched {len(results)} results for {row['strEvent']}")

    if not all_results:
        print("No results found.")
        return

    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    file_name = f"f1_race_results_{F1_SEASON}.json"
    local_path = LOCAL_DIR / file_name

    with open(local_path, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"Saved results locally to {local_path} — {len(all_results)} rows")
    return all_results


if __name__ == "__main__":
    client = storage.Client()

    races = fetch_and_save_f1_calendar()
    if races:
        upload_to_gcs(client, f"f1_calendar_{F1_SEASON}.json", LOCAL_DIR / f"f1_calendar_{F1_SEASON}.json")

    results = fetch_and_save_f1_results(races)
    if results:
        upload_to_gcs(client, f"f1_race_results_{F1_SEASON}.json", LOCAL_DIR / f"f1_race_results_{F1_SEASON}.json")
