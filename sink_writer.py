# sink_writer.py
import json
import logging
import os
from kafka import KafkaConsumer
import time
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sink_writer")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "cr_enriched"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "espn_enriched"
COLD_STORE_DIR = "./cold_store"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=1000
)

ch = Client(host=CLICKHOUSE_HOST)

def ensure_table():
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
        ts UInt64,
        player_id String,
        player_name String,
        team String,
        runs Nullable(Float64),
        balls Nullable(Float64),
        strike_rate Nullable(Float64),
        json String
    ) ENGINE = MergeTree()
    ORDER BY (player_id, ts)
    """
    ch.execute(create_sql)

def process_msg(msg):
    payload = msg.value
    ts = payload.get("ts", int(time.time() * 1000))
    player_id = str(payload.get("player_id", ""))
    player_name = payload.get("player_name", "")
    team = payload.get("team", "")
    runs = payload.get("runs")
    balls = payload.get("balls")
    strike_rate = payload.get("strike_rate")

    # insert into ClickHouse
    try:
        ch.execute(
            f"INSERT INTO {CLICKHOUSE_TABLE} (ts, player_id, player_name, team, runs, balls, strike_rate, json) VALUES",
            [(ts, player_id, player_name, team, runs, balls, strike_rate, json.dumps(payload))]
        )
    except Exception:
        logger.exception("Failed to insert into ClickHouse")

    # write to cold store (daily file)
    os.makedirs(COLD_STORE_DIR, exist_ok=True)
    day = time.strftime("%Y-%m-%d", time.localtime(ts / 1000.0))
    outpath = os.path.join(COLD_STORE_DIR, f"{day}.jsonl")
    with open(outpath, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")

def main():
    ensure_table()
    logger.info("Starting sink writer, consuming %s", KAFKA_TOPIC)
    while True:
        try:
            for msg in consumer:
                process_msg(msg)
        except Exception:
            logger.exception("Error consuming messages, retrying in 2s")
            time.sleep(2)

if __name__ == "__main__":
    main()
