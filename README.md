# Real Data Stream Project

## Overview
This project is a real-time data pipeline for cricket match events. It collects live and historical data, processes and enriches it, and stores the results for analytics and reporting.

## Architecture
```
[ESPN API / Betting APIs / Websocket feeds]
         │
 (producers / scrapers)
producer_espn.py (live)  <--- polls live endpoints
history_loader.py (one-off) ---> loads past matches
         │
         ▼
    Apache Kafka (topics)
 ┌─────────┬───────────────┬──────────────┐
 │ cr_live │ cr_history    │ cr_enriched  │
 └─────────┴───────────────┴──────────────┘
         │
         ▼
   Apache Flink Job(s)
- consume cr_live + cr_history
- enrich, aggregate, join
- output enriched events -> cr_enriched
         │
         ▼
   Downstream sinks
sink_writer.py consumes cr_enriched -> ClickHouse (hot)
sink_writer.py writes daily JSON files -> cold storage
         │
         ▼
Dashboards / APIs / ML Models / BI tools
```

## Components

### 1. `producer_testapi.py`
- Fetches football match data from Scorebat API
- Writes events to `api_test.jsonl`

### 2. `producer_footballapi2.py`
- Fetches football match data from TheSportsDB API
- Writes events to `api2.jsonl`

### 2. `history_loader.py`
- Loads historical match data from local JSON files
- Sends events to Kafka topic `cr_history`

### 3. `enrichment_watchdog.py`
- Monitors all API JSONL files
- Merges and enriches data for similar players/teams by unique ID
- Writes cumulative output to `enriched.jsonl`

### 4. `sink_writer.py` (optional)
- Can be adapted to consume from `enriched.jsonl` or Kafka topic
- Writes data to ClickHouse database
- Stores daily partitioned JSON files in `cold_store/`


## How to Run

1. **Start Docker Services**
   - Run `docker-compose up` to start Kafka, Zookeeper, and ClickHouse.

2. **Create Kafka Topics**
   - Run `create_topics.bat` to create required Kafka topics.

3. **Run Producers**
   - Start `producer_espn.py` for live data.
   - Run `history_loader.py` to load historical data (optional).

4. **Run Enrichment Job**
   - Start `enrichment_job.py` to process and enrich events.

5. **Run Sink Writer**
   - Start `sink_writer.py` to store enriched data in ClickHouse and cold storage.

## Requirements
- Python 3.8+
- Docker
- Kafka, ClickHouse (via Docker)
- Python packages: `kafka-python`, `clickhouse-driver`, `requests`

## Monitoring & Querying Data
- **ClickHouse Web UI:** Visit [http://localhost:8123](http://localhost:8123) (landing page only).
- **Run SQL queries:**
   - **Docker client:**
      ```powershell
      docker exec -it clickhouse clickhouse-client
      ```
      Then run SQL queries, e.g.:
      ```sql
      SELECT * FROM espn_enriched ORDER BY ts DESC LIMIT 10;
      ```
   - **HTTP request:**
      ```powershell
      curl -d "SELECT * FROM espn_enriched LIMIT 10" http://localhost:8123/
      ```
   - **Third-party tools:** Use Tabix, DBeaver, DataGrip, etc. to connect and query ClickHouse.
- **Cold Storage:** Check daily JSON files in the `cold_store/` directory.
- **Merged Output:** Check `enriched.jsonl` for cumulative, merged, and enriched data from all API sources.

## Notes
- All configuration is local by default (localhost).
- Adjust endpoints and settings as needed for your environment.

## License
MIT
