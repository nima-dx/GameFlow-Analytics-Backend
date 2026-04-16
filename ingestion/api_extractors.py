import json
import os
from pathlib import Path

import requests
# from dotenv import load_dotenv

# Load environment variables
# load_dotenv()

# Global variables
base_url = "https://www.thesportsdb.com/api/v2/json/"
api_key = os.getenv("API_KEY", "149076")
# output_dir = "/app/airflow/data/api-ingest"

output_dir = '/home/arthurdeetu/code/joaquin-ortega84/GameFlow-Analytics-Backend/joaquin/api_extractor_test'

F1_LEAGUE_ID = 4370
F1_SEASON = "2026"



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

    # Get list of existing JSON files
    existing_files = list_json_files()

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
                if dest_file_name not in existing_files:
                    write_json(src_url, dest_file_name)


def extract_event_timeline_data():
    # Find all files that start with "events-" in output_dir
    events_files = Path(output_dir).glob("events-*.json")

    # Get list of existing JSON files
    existing_files = list_json_files()

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
                if dest_file_name not in existing_files:
                    write_json(src_url, dest_file_name)


def extract_event_stats_data():
    # Find all files that start with "events-" in output_dir
    events_files = Path(output_dir).glob("events-*.json")

    # Get list of existing JSON files
    existing_files = list_json_files()

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
                if dest_file_name not in existing_files:
                    write_json(src_url, dest_file_name)


def extract_f1_calendar():
    url = (
        f"https://www.thesportsdb.com/api/v1/json/{api_key}/eventsseason.php"
        f"?id={F1_LEAGUE_ID}&s={F1_SEASON}"
    )
    response = requests.get(url)
    response.raise_for_status()

    events = response.json().get("events") or []
    races = [
        e for e in events
        if "grand prix" in e.get("strEvent", "").lower()
        and "qualifying" not in e.get("strEvent", "").lower()
        and "practice" not in e.get("strEvent", "").lower()
        and "sprint" not in e.get("strEvent", "").lower()
    ]

    if not races:
        print("No races found.")
        return

    output_file = Path(output_dir) / f"f1_calendar_{F1_SEASON}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(races, f, indent=2)

    print(f"Data saved to {output_file} — {len(races)} races")


def extract_f1_results():
    calendar_file = Path(output_dir) / f"f1_calendar_{F1_SEASON}.json"
    if not calendar_file.exists():
        print(f"F1 calendar file not found at {calendar_file}. Run 'f1 calendar' first.")
        return

    with open(calendar_file, "r") as f:
        races = json.load(f)

    completed = [r for r in races if r.get("strStatus") == "Match Finished"]
    if not completed:
        print("No completed races found.")
        return

    all_results = []
    for race in completed:
        event_id = race.get("idEvent")
        url = f"https://www.thesportsdb.com/api/v1/json/{api_key}/eventresults.php?id={event_id}"
        results = requests.get(url).json().get("results") or []
        all_results.extend(results)
        print(f"Fetched {len(results)} results for {race.get('strEvent')}")

    if not all_results:
        print("No results found.")
        return

    output_file = Path(output_dir) / f"f1_race_results_{F1_SEASON}.json"
    with open(output_file, "w") as f:
        json.dump(all_results, f, indent=2)

    print(f"Data saved to {output_file} — {len(all_results)} rows")


def list_json_files():
    """List all JSON files in the output directory.

    Returns:
        list: A sorted list of JSON file names in the output directory.
    """
    json_files = []
    output_path = Path(output_dir)

    # Check if the directory exists
    if not output_path.exists():
        print(f"Directory not found: {output_dir}")
        return json_files

    # Find all JSON files in the directory
    for json_file in output_path.glob("*.json"):
        json_files.append(json_file.name)

    return sorted(json_files)


# if __name__ == "__main__":
#     extract_f1_calendar()
#     extract_f1_results()
