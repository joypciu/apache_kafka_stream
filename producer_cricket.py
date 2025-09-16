import json
import time
import logging
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, KafkaError
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("producer_espn")


# Kafka configuration
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "cr_live"
POLL_INTERVAL = 10

# CricketData.org API configuration
API_KEY_PATH = "cricketdata_api_key.txt"
CRICKETDATA_SERIES_URL = "https://api.cricapi.com/v1/series"
CRICKETDATA_LIVE_URL = "https://api.cricapi.com/v1/currentMatches"

def load_api_key():
    with open(API_KEY_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                return line
    raise Exception("API key not found in cricketdata_api_key.txt")

# Initialize Kafka producer with retry logic
def create_producer():
    retries = 5
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=5,
                max_block_ms=15000,
                metadata_max_age_ms=30000
            )
            logger.info("Successfully connected to Kafka broker at %s", KAFKA_BOOTSTRAP)
            return producer
        except KafkaTimeoutError as e:
            logger.warning("Attempt %d/%d failed to connect to Kafka: %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(5)
            else:
                logger.error("Failed to connect to Kafka after %d attempts", retries)
                raise
        except Exception as e:
            logger.error("Unexpected error connecting to Kafka: %s", e)
            raise

def fetch_live_matches(api_key):
    try:
        params = {"apikey": api_key}
        r = requests.get(CRICKETDATA_LIVE_URL, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "success" and "data" in data:
            return data["data"]
        else:
            logger.warning("No live match data returned: %s", data)
            return []
    except Exception as e:
        logger.exception("Failed to fetch CricketData.org live matches: %s", e)
        return []

def extract_events_from_cricketdata(matches):
    events = []
    for match in matches:
        match_id = match.get("id") or match.get("matchId")
        competition = match.get("name") or match.get("seriesName")
        ts = int(time.time() * 1000)
        # Match snapshot only
        snapshot = {
            "type": "match_snapshot",
            "match_id": match_id,
            "competition": competition,
            "raw": match,
            "ts": ts
        }
        events.append(snapshot)
        # Player stats not available in this API response
    return events

def main():
    producer = None
    try:
        logger.info("Waiting for Kafka to initialize...")
        time.sleep(10)
        api_key = load_api_key()
        producer = create_producer()
        if producer is None:
            logger.error("Kafka producer could not be created. Exiting.")
            return
        logger.info("Starting CricketData.org producer to topic %s", KAFKA_TOPIC)
        while True:
            matches = fetch_live_matches(api_key)
            logger.info(f"Raw matches response: {matches}")
            if not isinstance(matches, list):
                logger.error(f"Expected a list of matches, got {type(matches)}: {matches}")
                time.sleep(POLL_INTERVAL)
                continue
            events = extract_events_from_cricketdata(matches)
            logger.info("Fetched %d events from CricketData.org", len(events))
            for event in events:
                try:
                    producer.send(KAFKA_TOPIC, event)
                    logger.info("✅ Produced: %s", event)
                except KafkaError as e:
                    logger.error("Failed to send event to Kafka: %s", e)
            producer.flush()
            time.sleep(POLL_INTERVAL)
    except Exception as e:
        logger.error("Producer failed: %s", e)
    finally:
        if producer is not None:
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    main()