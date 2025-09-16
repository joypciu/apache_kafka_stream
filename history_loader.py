# history_loader.py
import json
import glob
from kafka import KafkaProducer
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("history_loader")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "cr_history"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def load_files(path_glob="history/*.json"):
    for path in glob.glob(path_glob):
        logger.info("Loading %s", path)
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            # assume file contains array of events
            if isinstance(data, list):
                for e in data:
                    producer.send(KAFKA_TOPIC, e)
            else:
                producer.send(KAFKA_TOPIC, data)
            producer.flush()
            time.sleep(0.5)

if __name__ == "__main__":
    load_files()
    logger.info("Finished loading history.")
