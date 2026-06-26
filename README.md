# Wikipedia Anomaly Pipeline

Real-time pipeline that streams Wikipedia edit events, detects edit volume anomalies and bot activity using Z-score analysis, and displays results in a live Streamlit dashboard.

## Architecture

```
Wikimedia SSE Stream
        │
        ▼
  wiki_producer.py          ← Python on AWS EC2 (systemd)
        │  Kafka topics: wiki_edits, wiki_page_events
        ▼
  Confluent Cloud Kafka
        │
        ▼
  01_bronze_ingestion.py    ← Databricks: raw Delta tables
        │
        ▼
  02_silver_features.py     ← Databricks: feature engineering
        │
        ▼
  03_gold_aggregates.py     ← Databricks: windowed aggregates + anomaly flags
        │
        ▼
  dashboard/app.py          ← Databricks App (Streamlit)
```

## Components

### Producer (`producer/`)
Streams from the [Wikimedia recent changes SSE feed](https://stream.wikimedia.org/v2/stream/recentchange), filters to English Wikipedia, and publishes to two Confluent Cloud Kafka topics:
- `wiki_edits` — edit and new-page events
- `wiki_page_events` — categorize and log events

Runs as a systemd service on an AWS EC2 instance for 24/7 uptime with auto-restart.

### Bronze Layer (`databricks/01_bronze_ingestion.py`)
Reads from Kafka using Structured Streaming with `trigger(availableNow=True)` and writes raw JSON payloads to Delta tables:
- `bronze_wiki_edits`
- `bronze_wiki_page_events`

### Silver Layer (`databricks/02_silver_features.py`)
Incremental feature engineering on top of bronze. Adds per-row derived columns:

| Column | Description |
|---|---|
| `edit_timestamp` | Unix epoch → TimestampType |
| `edit_hour` / `edit_day_of_week` | Time features |
| `bytes_delta` / `abs_bytes_delta` | Size change of edit |
| `namespace_label` | Human-readable namespace (Article, Talk, User, …) |
| `is_revert` | Detected via regex on edit comment |
| `silver_processed_at` | Processing latency tracking |

Writes to `silver_wiki_edits` using Structured Streaming with checkpoint-based incremental processing.

### Gold Layer (`databricks/03_gold_aggregates.py`)
Batch aggregation over the last 2 days of silver data into two tables:

**`gold_edit_volume`** — tumbling window aggregates at 1-min and 5-min granularity with Z-score anomaly flagging:
- Rolling 1-hour baseline mean and stddev per window
- `is_anomaly = total_edits > mean + 2 * stddev`
- `z_score` for severity ranking

**`gold_user_activity`** — per-user per-window edit counts, bytes changed, revert counts, and unique pages edited.

Both tables are partitioned by `date` for efficient incremental overwrites and fast dashboard queries.

### Dashboard (`dashboard/`)
Streamlit app deployed on Databricks Apps with four charts:
- **Edit volume time series** with rolling mean, 2σ threshold, and flagged anomaly markers
- **Bot vs human edit ratio** stacked bar with bot ratio trend line
- **Z-score over time** with anomaly zone shading
- **Top editors during anomalous windows** ranked bar chart

Sidebar controls: window granularity (1min / 5min), lookback hours (1–48), auto-refresh toggle.

## Setup

### Prerequisites
- Confluent Cloud account (free tier) with a Kafka cluster
- AWS account with an EC2 instance (t3.micro free tier)
- Databricks account (free edition)

### 1. Producer on EC2

```bash
# SSH into your EC2 instance, then run:
bash infra/producer-setup.sh
```

The setup script installs dependencies, clones the repo, prompts for Confluent Cloud credentials, and installs a systemd service that runs the producer continuously.

```bash
# Check status
sudo systemctl status wiki-producer

# View live logs
sudo journalctl -u wiki-producer -f
```

### 2. Databricks Secret Scope

Create a secret scope named `wikipedia-anomaly-pipeline` and add:

| Key | Value |
|---|---|
| `kafka-bootstrap-server` | Confluent Cloud bootstrap server |
| `kafka-api-key` | Confluent Cloud API key |
| `kafka-api-secret` | Confluent Cloud API secret |
| `warehouse-http-path` | Databricks SQL warehouse HTTP path |

### 3. Databricks Notebooks

Run in order:
1. `databricks/01_bronze_ingestion.py`
2. `databricks/02_silver_features.py`
3. `databricks/03_gold_aggregates.py`

Import each `.py` file into Databricks as a notebook (File → Import → select file).

### 4. Dashboard

Deploy `dashboard/` as a Databricks App:
- Source: this GitHub repo, `main` branch, source path `dashboard`
- Add your SQL warehouse as an App resource (grants the app's service principal query access)
- Grant the app's service principal READ access to the `wikipedia-anomaly-pipeline` secret scope

## Repository Structure

```
wikipedia-anomaly-pipeline/
├── producer/
│   └── wiki_producer.py          # Wikimedia SSE → Kafka producer
├── databricks/
│   ├── 01_bronze_ingestion.py    # Kafka → Delta (raw)
│   ├── 02_silver_features.py     # Feature engineering
│   └── 03_gold_aggregates.py     # Windowed aggregates + anomaly detection
├── dashboard/
│   ├── app.py                    # Streamlit dashboard
│   ├── app.yaml                  # Databricks Apps config
│   └── requirements.txt
├── infra/
│   ├── producer-setup.sh         # EC2 setup script
│   └── wiki-producer.service     # systemd service definition
└── requirements.txt              # Producer dependencies
```
