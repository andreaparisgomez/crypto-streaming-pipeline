# Setup Guide

## Clone Repository

```bash
git clone <repo-url>
cd crypto-streaming-pipeline
```

---

## Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

---

## Install Dependencies

```bash
pip install -r requirements.txt
```

---

## Environment Variables

Create a `.env` file in the project root.

Example:

```env
POSTGRES_HOST=localhost
POSTGRES_DB=crypto_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password

WAREHOUSE_HOST=your_neon_host
WAREHOUSE_DB=neondb
WAREHOUSE_USER=your_neon_user
WAREHOUSE_PASSWORD=your_neon_password
WAREHOUSE_SSLMODE=require

KAFKA_BOOTSTRAP_SERVERS=localhost:9092

YOUTUBE_API_KEY=your_youtube_api_key
```

The real `.env` files are excluded from Git using `.gitignore`.

Template configuration files are provided through:

```text
.env.example
airflow/.env.example
```

---

## Kafka Setup

Kafka runs inside Docker containers.

Start Kafka services:

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

Create Kafka topics:

```bash
bash scripts/create_kafka_topics.sh

bash scripts/create_sentiment_kafka_topics.sh
```

Inspect Kafka topics if needed:

```bash
bash scripts/inspect_kafka_topics.sh

bash scripts/inspect_sentiment_topics.sh
```

---

## PostgreSQL Setup

Create the operational PostgreSQL database:

```sql
CREATE DATABASE crypto_db;
```

Run the SQL setup scripts:

```bash
psql -U postgres -d crypto_db \
  -f sql/create_crypto_metrics_table.sql

psql -U postgres -d crypto_db \
  -f sql/create_daily_crypto_summary_table.sql

psql -U postgres -d crypto_db \
  -f sql/create_youtube_sentiment_tables.sql

psql -U postgres -d crypto_db \
  -f sql/create_historical_crypto_prices_table.sql
```

---

## Neon Warehouse Setup

Create a Neon PostgreSQL database and update the warehouse environment variables.

Run the warehouse setup scripts:

```bash
psql "postgresql://USER:PASSWORD@HOST/neondb?sslmode=require" \
  -f sql/create_neon_warehouse_schema.sql

psql "postgresql://USER:PASSWORD@HOST/neondb?sslmode=require" \
  -f sql/create_dashboard_views.sql
```

Optional warehouse validation queries:

```bash
psql "postgresql://USER:PASSWORD@HOST/neondb?sslmode=require" \
  -f sql/warehouse_validation_queries.sql
```

---

## Airflow Setup

Navigate to the Airflow directory:

```bash
cd airflow
```

Start Airflow services:

```bash
docker compose up -d
```

Airflow UI:

```text
http://localhost:8080
```

---

## Airflow Environment Variables

Airflow services receive PostgreSQL and warehouse credentials through Docker Compose environment variables defined in:

```text
airflow/.env
```

Example:

```env
POSTGRES_HOST=host.docker.internal
POSTGRES_DB=crypto_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password

WAREHOUSE_HOST=your_neon_host
WAREHOUSE_DB=neondb
WAREHOUSE_USER=your_neon_user
WAREHOUSE_PASSWORD=your_neon_password
WAREHOUSE_SSLMODE=require
```

Environment variables are propagated into the Airflow containers during container initialisation.

---

## Spark Version Compatibility

This project uses:

- PySpark 4.1.1
- Kafka connector:
  `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1`

Matching Spark and Kafka connector versions are required for successful streaming execution.

---

## Running the Cryptocurrency Streaming Pipeline

Open separate terminal sessions.

### 1. Kafka Producer

```bash
python crypto-market-stream/producer.py
```

---

### 2. Spark Structured Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  crypto-market-stream/spark_processor.py
```

---

### 3. PostgreSQL Consumer

```bash
python crypto-market-stream/postgre_consumer.py
```

---

## Running the Sentiment Streaming Pipeline

Open separate terminal sessions.

### 1. YouTube Producer

```bash
python crypto-market-stream/youtube_producer.py
```

---

### 2. Spark Sentiment Processor

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  crypto-market-stream/youtube_sentiment_spark_processor.py
```

---

### 3. PostgreSQL Sentiment Consumer

```bash
python crypto-market-stream/youtube_postgres_consumer.py
```

---

## Running Airflow Workflows

Trigger DAGs from the Airflow UI as needed.

Example DAGs include:

- `daily_crypto_summary`
- `daily_youtube_sentiment_summary`
- `load_crypto_fact_table`
- `load_youtube_sentiment_warehouse`
- platform monitoring DAGs

---

## Verifying Streaming Output

Connect to PostgreSQL:

```bash
PGPASSWORD=your_postgres_password \
psql -U postgres -d crypto_db
```

Check crypto streaming inserts:

```sql
SELECT COUNT(*) FROM crypto_metrics;
```

Check sentiment streaming inserts:

```sql
SELECT COUNT(*) FROM youtube_sentiment_metrics;
```

Check daily summaries:

```sql
SELECT * FROM daily_crypto_summary;
```

---

## Dashboard Visualisation

The project includes Looker Studio dashboards connected directly to the Neon analytical warehouse.

Dashboard walkthroughs and screenshots are documented in:

```text
docs/dashboard_walkthrough.md
```

---

## Useful Kafka Utility Scripts

Consume streaming topics directly from Kafka:

```bash
bash scripts/consume_crypto_metrics.sh

bash scripts/consume_youtube_raw_comments.sh

bash scripts/consume_youtube_sentiment.sh
```

These scripts help validate streaming behaviour during development and debugging.
