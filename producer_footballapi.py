# producer_testapi.py
import requests
import json
import time

API_FILE = "api_football_test.jsonl"
API_URL = "https://www.scorebat.com/video-api/v3/feed/"  # Free public football highlights API

def fetch_football_data():
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        # The API returns a dict with 'response' key containing a list of matches
        return data.get("response", [])
    except Exception as e:
        print(f"Error fetching football data: {e}")
        return []

def main():
    print("Starting football API producer...")
    while True:
        matches = fetch_football_data()
        with open(API_FILE, "a", encoding="utf-8") as out:
            for match in matches:
                # Example: extract match_id, teams, and any available player info
                record = {
                    "match_id": match.get("title"),
                    "team1": match.get("side1", {}).get("name"),
                    "team2": match.get("side2", {}).get("name"),
                    "competition": match.get("competition", {}).get("name"),
                    "date": match.get("date"),
                    "video_url": match.get("videos", [{}])[0].get("embed"),
                    "ts": int(time.time() * 1000)
                }
                out.write(json.dumps(record) + "\n")
        print(f"Wrote batch to {API_FILE}")
        time.sleep(60)

if __name__ == "__main__":
    main()
