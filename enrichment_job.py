# enrichment_job.py
import json
import logging
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("enrichment_job")

KAFKA_BOOTSTRAP = "localhost:9092"
LIVE_TOPIC = "cr_live"
HISTORY_TOPIC = "cr_history"
OUT_TOPIC = "cr_enriched"
GROUP_ID = "python-enrichment-group"

consumer = KafkaConsumer(
    LIVE_TOPIC,
    HISTORY_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id=GROUP_ID,
    consumer_timeout_ms=1000
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def enrich_event(event):
    etype = event.get("type", "")
    if etype == "player_snapshot":
        runs = event.get("stats", {}).get("runs")
        balls = event.get("stats", {}).get("balls")
        enriched = {
            "player_id": event.get("player_id"),
            "player_name": event.get("player_name"),
            "team": event.get("team"),
            "runs": runs,
            "balls": balls,
            "type": "player_enriched",
            "ts": event.get("ts")
        }
        if runs is not None and balls:
            try:
                enriched["strike_rate"] = (float(runs) / max(1.0, float(balls))) * 100.0
            except Exception:
                pass
        return enriched
    elif etype == "match_snapshot":
        return {
            "type": "match_enriched",
            "match_id": event.get("match_id"),
            "competition": event.get("competition"),
            "ts": event.get("ts")
        }
    else:
        return {"type": "unknown", "raw": event}

def main():
    logger.info("Starting Python enrichment job...")
    while True:
        try:
            for msg in consumer:
                event = msg.value
                enriched = enrich_event(event)
                producer.send(OUT_TOPIC, enriched)
                logger.info(f"✅ Enriched and produced: {enriched}")
            producer.flush()
        except Exception:
            logger.exception("Error in enrichment job, retrying in 2s")
            time.sleep(2)

if __name__ == "__main__":
    main()
