# Guide to Running the Real Data Stream Project

This guide will help you set up and run the entire cricket data pipeline, from fetching live data to storing enriched results.

---

## 1. Prerequisites
- **Python 3.8+** installed
- **Docker** installed and running
- **Kafka, ClickHouse** (will be started via Docker)
- **API Key for CricketData.org** (place in `cricketdata_api_key.txt`)
- **Required Python packages:**
  - `kafka-python`
  - `clickhouse-driver`
  - `pyflink`
  - `requests`

Install packages:
```powershell
pip install kafka-python clickhouse-driver pyflink requests
```

---

## 2. Start Infrastructure

1. **Start Docker Services**
   - Open a terminal in the project directory.
   - Run:
     ```powershell
     docker-compose up -d
     ```
  - This starts Kafka, Zookeeper, and ClickHouse containers.

2. **Create Kafka Topics**
   - Run the batch file:
     ```powershell
     .\create_topics.bat
     ```
   - This creates the required topics: `cr_live`, `cr_history`, `cr_enriched`.

---

## 3. Prepare API Key
- Place your CricketData.org API key in `cricketdata_api_key.txt` (one line, no quotes).

---

## 4. Run Producers

### a. Football API Producers
- Start the first football API producer:
  ```powershell
  python producer_testapi.py
  ```
- Start the second football API producer:
  ```powershell
  python producer_footballapi2.py
  ```
- Each producer fetches football match data from a different API and writes to its own JSONL file (`api_test.jsonl`, `api2.jsonl`).

### b. History Loader (Optional)
- If you have historical match data in JSON files, run:
  ```powershell
  python history_loader.py
  ```
- This will send historical events to Kafka topic `cr_history`.

---

## 5. Run Enrichment Job

Start the enrichment watchdog job:
  ```powershell
  python enrichment_watchdog.py
  ```
This will monitor all API JSONL files, merge and enrich data for similar players/teams by unique ID, and write the cumulative output to `enriched.jsonl`.

---

## 6. Run Sink Writer
*If you want to store merged data in ClickHouse and cold storage, you can adapt `sink_writer.py` to consume from `enriched.jsonl` or continue using Kafka topics as before.*

---

## 7. Monitor & Access Data
- **ClickHouse**:
  - Access the web UI at [http://localhost:8123](http://localhost:8123) (landing page only).
  - To run SQL queries, use one of these methods:
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

**Cold Storage**: Check daily JSON files in the `cold_store/` directory.

---

## 8. Troubleshooting
- Check logs in the terminal for errors.
- Ensure Docker containers are running (`docker ps`).
- Verify API key is correct and valid.
- Make sure Kafka topics exist (`create_topics.bat`).

---

## 9. Stopping Services
- To stop all Docker containers:
  ```powershell
  docker-compose down
  ```

---

## 10. Notes
- You can customize polling intervals and endpoints in the Python scripts.
- All components are designed to run independently; restart any if needed.

---

Enjoy your real-time cricket data pipeline!
