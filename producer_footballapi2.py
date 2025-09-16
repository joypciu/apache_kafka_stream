# producer_footballapi2.py
import requests
import json
import time

API_FILE = "api_football_test2.jsonl"
API_URL = "https://www.thesportsdb.com/api/v1/json/1/eventspastleague.php?id=4328"  # English Premier League recent events

def fetch_football_data():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        # The API returns a dict with 'events' key containing a list of matches
        return data.get("events", [])
    except Exception as e:
        print(f"Error fetching football data: {e}")
        return []


def main():
    print("Starting football API 2 producer...")
    while True:
        matches = fetch_football_data()
        with open(API_FILE, "a", encoding="utf-8") as out:
            for match in matches:
                record = {
                    "match_id": match.get("idEvent"),
                    "team1": match.get("strHomeTeam"),
                    "team2": match.get("strAwayTeam"),
                    "competition": match.get("strLeague"),
                    "date": match.get("dateEvent"),
                    "score_home": match.get("intHomeScore"),
                    "score_away": match.get("intAwayScore"),
                    "ts": int(time.time() * 1000)
                }
                out.write(json.dumps(record) + "\n")
        print(f"Wrote batch to {API_FILE}")
        time.sleep(60)

if __name__ == "__main__":
    main()
