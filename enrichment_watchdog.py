# enrichment_watchdog.py
import json
import os
import time
from collections import defaultdict

# List your API JSONL files here
API_FILES = ["api_test.jsonl", "api2.jsonl"]
ENRICHED_FILE = "enriched.jsonl"
CHECK_INTERVAL = 10  # seconds

# In-memory store for latest player/team data by unique id
latest_data = defaultdict(dict)

# Track last read position for each file
file_positions = {f: 0 for f in API_FILES}

def process_new_lines(file_path, last_pos):
    new_records = []
    with open(file_path, "r", encoding="utf-8") as fh:
        fh.seek(last_pos)
        for line in fh:
            try:
                record = json.loads(line.strip())
                new_records.append(record)
            except Exception:
                continue
        new_pos = fh.tell()
    return new_records, new_pos

def update_latest_data(records):
    for rec in records:
        # Assume each record has 'player_id', 'team_id', and other fields
        player_id = rec.get("player_id")
        team_id = rec.get("team_id")
        if player_id:
            latest_data["player"][player_id] = rec
        if team_id:
            latest_data["team"][team_id] = rec


def write_enriched():
    with open(ENRICHED_FILE, "w", encoding="utf-8") as out:
        # Write all latest player and team records
        for player in latest_data["player"].values():
            out.write(json.dumps(player) + "\n")
        for team in latest_data["team"].values():
            out.write(json.dumps(team) + "\n")


def main():
    print("Starting enrichment watchdog...")
    while True:
        for f in API_FILES:
            if not os.path.exists(f):
                continue
            records, new_pos = process_new_lines(f, file_positions[f])
            if records:
                update_latest_data(records)
                write_enriched()
            file_positions[f] = new_pos
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()
