#!/bin/bash
set -e
KAFKA_DIR=./kafka_tools_tmp

# function uses kafka-topics shell available in the kafka container image
docker exec -it kafka bash -c "kafka-topics.sh --create --topic cr_live --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true"
docker exec -it kafka bash -c "kafka-topics.sh --create --topic cr_history --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true"
docker exec -it kafka bash -c "kafka-topics.sh --create --topic cr_enriched --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 || true"

echo "Topics created (or already existed)."
