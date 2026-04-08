import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Global variables
base_url = "https://www.thesportsdb.com/api/v2/json/"
api_key = os.getenv("API_KEY", "149076")
output_dir = "/app/airflow/data/api-ingest"


def fetch_and_save_leagues():
    """Fetch leagues data from TheSportsDB API and save to JSON file."""
    leagues_url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/all_leagues.php"

    # Make the API request
    response = requests.get(leagues_url)
    response.raise_for_status()

    # Get the JSON data
    data = response.json()

    # Create the directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Save to file
    output_file = Path(output_dir) / "leagues.json"
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)

    print(f"Successfully saved leagues data to {output_file}")
    print(f"Total leagues fetched: {len(data.get('leagues', []))}")


def write_json(src_url, dest_file_name):

    headers = {
        "X-API-KEY": f"{api_key}",
        "Content-Type": "application/json"
    }

    url_ = base_url + "/" + src_url
    response = requests.get(url_, headers=headers)

    if response.status_code == 200:
        # Create parent folder if it doesn't exist
        output_file = Path(output_dir) / dest_file_name
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Save JSON to file
        with open(output_file, "w") as f:
            json.dump(response.json(), f, indent=2)

        print(f"Data saved to {output_file}")
    else:
        print(f"Request failed with status code: {response.status_code}")


def entity_caller(entity):

    if entity == "leagues":
        extract_leagues_data()

    elif entity == "teams":
        extract_teams_data()

    elif entity == "players":
        extract_players_data()

    elif entity == "seasons":
        extract_seasons_data()

    elif entity == "events":
        extract_league_all_events()

    elif entity == "event timelines":
        extract_event_timeline_data()

    elif entity == "event stats":
        extract_event_stats_data()
    else:
        print(f"Unknown entity: {entity}")


def extract_leagues_data():
    src_url = "all/leagues"
    dest_file_name = "leagues.json"
    write_json(src_url, dest_file_name)

def extract_teams_data():
    leagues_file = Path(output_dir) / "leagues.json"

    # Check if leagues.json file exists
    if not leagues_file.exists():
        print(f"Leagues file not found at {leagues_file}. Exiting.")
        return

    # Read the leagues file
    with open(leagues_file, "r") as f:
        leagues_data = json.load(f)

    # Target leagues to filter
    target_leagues = ["Italian Serie A", "French Ligue 1", "Spanish La Liga"]

    # Filter leagues and extract idLeague
    league_ids = []
    for league in leagues_data.get("all", []):
        if league.get("strLeague") in target_leagues:
            league_ids.append(league.get("idLeague"))

    # Build URLs and call write_json for each league
    for league_id in league_ids:
        src_url = f"list/teams/{league_id}"
        dest_file_name = f"teams-{league_id}.json"
        write_json(src_url, dest_file_name)


def extract_players_data():
    # Find all files that start with "teams" in output_dir
    teams_files = Path(output_dir).glob("teams*.json")

    for teams_file in teams_files:
        # Read the teams file
        with open(teams_file, "r") as f:
            teams_data = json.load(f)

        # Iterate through the list of teams
        for team in teams_data.get("list", []):
            team_id = team.get("idTeam")
            if team_id:
                # Build URL and call write_json for each team
                src_url = f"list/players/{team_id}"
                dest_file_name = f"players-{team_id}.json"
                write_json(src_url, dest_file_name)


def extract_seasons_data():
    leagues_file = Path(output_dir) / "leagues.json"

    # Check if leagues.json file exists
    if not leagues_file.exists():
        print(f"Leagues file not found at {leagues_file}. Exiting.")
        return

    # Read the leagues file
    with open(leagues_file, "r") as f:
        leagues_data = json.load(f)

    # Target leagues to filter
    target_leagues = ["Italian Serie A", "French Ligue 1", "Spanish La Liga"]

    # Filter leagues and extract idLeague
    league_ids = []
    for league in leagues_data.get("all", []):
        if league.get("strLeague") in target_leagues:
            league_ids.append(league.get("idLeague"))

    # Build URLs and call write_json for each league
    for league_id in league_ids:
        src_url = f"list/seasons/{league_id}"
        dest_file_name = f"seasons-{league_id}.json"
        write_json(src_url, dest_file_name)


def extract_league_all_events():
    leagues_file = Path(output_dir) / "leagues.json"

    # Check if leagues.json file exists
    if not leagues_file.exists():
        print(f"Leagues file not found at {leagues_file}. Exiting.")
        return

    # Read the leagues file
    with open(leagues_file, "r") as f:
        leagues_data = json.load(f)

    # Target leagues and seasons to filter
    target_leagues = ["Italian Serie A", "French Ligue 1", "Spanish La Liga"]
    # target_seasons = ["2021-2022", "2022-2023", "2023-2024", "2024-2025", "2025-2026"]
    target_seasons = ["2022-2023", "2023-2024", "2024-2025"]

    # Filter leagues and extract idLeague
    league_ids = []
    for league in leagues_data.get("all", []):
        if league.get("strLeague") in target_leagues:
            league_ids.append(league.get("idLeague"))

    # For each league, read its seasons file and build URLs
    for league_id in league_ids:
        seasons_file = Path(output_dir) / f"seasons-{league_id}.json"

        # Check if seasons file exists for this league
        if not seasons_file.exists():
            print(f"Seasons file not found at {seasons_file}. Skipping league {league_id}.")
            continue

        # Read the seasons file
        with open(seasons_file, "r") as f:
            seasons_data = json.load(f)

        # Filter for target seasons and build URLs
        for season in seasons_data.get("list", []):
            season_id = season.get("strSeason")
            if season_id in target_seasons:
                src_url = f"schedule/league/{league_id}/{season_id}"
                dest_file_name = f"events-{league_id}-{season_id}.json"
                write_json(src_url, dest_file_name)


def extract_event_timeline_data():
    # Find all files that start with "events-" in output_dir
    events_files = Path(output_dir).glob("events-*.json")

    for events_file in events_files:
        # Read the events file
        with open(events_file, "r") as f:
            events_data = json.load(f)

        # Iterate through the schedule of events
        for event in events_data.get("schedule", []):
            event_id = event.get("idEvent")
            if event_id:
                # Build URL and call write_json for each event
                src_url = f"lookup/event_timeline/{event_id}"
                dest_file_name = f"event-timeline-{event_id}.json"
                write_json(src_url, dest_file_name)


def extract_event_stats_data():
    # Find all files that start with "events-" in output_dir
    events_files = Path(output_dir).glob("events-*.json")

    for events_file in events_files:
        # Read the events file
        with open(events_file, "r") as f:
            events_data = json.load(f)

        # Iterate through the schedule of events
        for event in events_data.get("schedule", []):
            event_id = event.get("idEvent")
            if event_id:
                # Build URL and call write_json for each event
                src_url = f"lookup/event_stats/{event_id}"
                dest_file_name = f"event-stats-{event_id}.json"
                write_json(src_url, dest_file_name)
