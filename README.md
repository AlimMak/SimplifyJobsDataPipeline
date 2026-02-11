# New Grad Job Listings - Apache Kafka Data Pipeline

## Architecture Overview

```
┌─────────────────┐    ┌───────────────┐    ┌─────────────────────────┐
│  GitHub README   │───▶│   Producer    │───▶│     Kafka Broker        │
│  (Data Source)   │    │  (Scraper +   │    │                         │
│                  │    │   Cleaner)    │    │  Topics:                │
└─────────────────┘    └───────────────┘    │  ├─ raw-job-listings    │
                                            │  ├─ cleaned-jobs        │
                                            │  ├─ jobs-by-category    │
                                            │  └─ job-alerts          │
                                            └──────────┬──────────────┘
                                                       │
                              ┌─────────────────────────┼────────────────────┐
                              │                         │                    │
                         ┌────▼─────┐            ┌──────▼──────┐    ┌───────▼──────┐
                         │ Consumer │            │  Consumer   │    │   Consumer   │
                         │ Group 1  │            │  Group 2    │    │   Group 3    │
                         │ (Store)  │            │ (Analytics) │    │  (Alerts)    │
                         └────┬─────┘            └──────┬──────┘    └───────┬──────┘
                              │                         │                    │
                         ┌────▼─────┐            ┌──────▼──────┐    ┌───────▼──────┐
                         │  SQLite  │            │  Terminal   │    │  Alert Log   │
                         │    DB    │            │  Dashboard  │    │  (Filtered)  │
                         └──────────┘            └─────────────┘    └──────────────┘
```

## Kafka Concepts Demonstrated

| Concept          | Where It's Used                                                   |
|------------------|-------------------------------------------------------------------|
| **Producer**     | `producer.py` — Scrapes GitHub, cleans data, publishes to topics  |
| **Consumer**     | `consumer_store.py`, `consumer_analytics.py`, `consumer_alerts.py`|
| **Topics**       | `raw-job-listings`, `cleaned-jobs`, `jobs-by-category`, `job-alerts` |
| **Consumer Groups** | 3 independent groups processing the same data differently      |
| **Partitions**   | Jobs partitioned by category for parallel processing              |
| **Serialization**| JSON serialization/deserialization of job records                  |

## Project Structure

```
kafka-pipeline/
├── README.md
├── docker-compose.yml          # Kafka + Zookeeper infrastructure
├── requirements.txt
├── src/
│   ├── config.py               # Kafka configuration & topic definitions
│   ├── scraper.py              # GitHub README HTML table parser & cleaner
│   ├── models.py               # Job data models
│   ├── producer.py             # Kafka producer - publishes job listings
│   ├── consumer_store.py       # Consumer Group 1 - persists to SQLite
│   ├── consumer_analytics.py   # Consumer Group 2 - real-time analytics
│   ├── consumer_alerts.py      # Consumer Group 3 - filtered job alerts
│   ├── stream_processor.py     # Kafka Streams-style processor (raw → cleaned)
│   ├── admin_topics.py         # Topic creation & management
│   └── run_pipeline.py         # Orchestrator to run the full pipeline
└── data/
    └── jobs.db                 # SQLite database (created at runtime)
```

## Quick Start

### 1. Start Kafka Infrastructure
```bash
docker-compose up -d
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Create Kafka Topics
```bash
python src/admin_topics.py
```

### 4. Run the Full Pipeline
```bash
python src/run_pipeline.py
```

### Or Run Components Individually
```bash
# Terminal 1 - Start the stream processor (raw → cleaned)
python src/stream_processor.py

# Terminal 2 - Start consumer group 1 (SQLite storage)
python src/consumer_store.py

# Terminal 3 - Start consumer group 2 (analytics dashboard)
python src/consumer_analytics.py

# Terminal 4 - Start consumer group 3 (job alerts)
python src/consumer_alerts.py

# Terminal 5 - Run the producer (scrape & publish)
python src/producer.py
```

## Data Cleaning Pipeline

The raw GitHub README contains HTML table rows with embedded links, emojis, and
formatting. The pipeline performs these cleaning steps:

1. **HTML Parsing** — Extracts table rows from the `<tbody>` elements
2. **Company Extraction** — Strips Simplify tracking links, extracts company name
3. **FAANG Detection** — Identifies 🔥 emoji markers for FAANG+ companies
4. **Sponsorship Flags** — Detects 🛂 (no sponsorship), 🇺🇸 (US citizenship), 🔒 (closed), 🎓 (advanced degree)
5. **Location Normalization** — Handles multi-location `<details>` tags, `</br>` splits
6. **Application URL Extraction** — Pulls direct apply links from nested `<a>` tags
7. **Age Parsing** — Converts "0d", "1mo" etc. to days-ago integer
8. **Category Tagging** — Assigns jobs to categories based on section headers
9. **Deduplication** — Removes duplicate listings (same company + role + location)

## Configuration

Edit `src/config.py` to customize:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Alert filters
ALERT_LOCATIONS = ["Remote", "SF", "NYC", "Austin"]
ALERT_COMPANIES = ["Google", "Microsoft", "Amazon", "Meta"]
ALERT_MAX_AGE_DAYS = 7
```
