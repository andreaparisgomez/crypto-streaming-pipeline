# Setup Guide

## Clone Repository

```bash
git clone <repo-url>
cd crypto-streaming-pipeline
```

---

# Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate
```

---

# Install Dependencies

```bash
pip install -r requirements.txt
```

---

# Environment Variables

Create a `.env` file in the project root:

```env
POSTGRES_HOST=localhost
POSTGRES_DB=crypto_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

---

# Kafka Setup

Kafka runs in Docker.

Start Kafka services:

```bash
docker start broker
```

Verify container is running:

```bash
docker ps
```

---

# PostgreSQL Setup

Create the database:

```sql
CREATE DATABASE crypto_db;
```

Create tables:

```bash
psql -U postgres -d crypto_db \
  -f sql/create_crypto_metrics_table.sql

psql -U postgres -d crypto_db \
  -f sql/create_daily_crypto_summary_table.sql
```

---

# Airflow Setup

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

Airflow services receive PostgreSQL credentials through Docker Compose environment variables defined in:

```text
airflow/.env
```

These variables are injected into the Airflow containers through the shared Docker Compose environment configuration.

Example:

```env
POSTGRES_HOST=host.docker.internal
POSTGRES_DB=crypto_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_postgres_password
```

The real `.env` files are excluded from Git using `.gitignore`.

Template configuration files are provided through:

```text
.env.example
airflow/.env.example
```
---

# Running the Streaming Pipeline

Open separate terminal sessions.

## 1. Producer

```bash
python producer.py
```

## 2. Spark Structured Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  spark_processor.py
```

## 3. PostgreSQL Consumer

```bash
python postgre_consumer.py
```

---

# Running Airflow Analytics

Trigger DAG:

```text
daily_crypto_summary
```

from the Airflow UI.

---

# Verifying Streaming Output

Connect to PostgreSQL:

```bash
PGPASSWORD=your_password \
psql -U postgres -d crypto_db
```

Check streaming inserts:

```sql
SELECT COUNT(*) FROM crypto_metrics;
```

Check daily summaries:

```sql
SELECT * FROM daily_crypto_summary;
```
