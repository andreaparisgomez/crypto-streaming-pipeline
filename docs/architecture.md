# Crypto Streaming Pipeline — Architecture

## Overview

This project is a hybrid real-time and batch data engineering pipeline for cryptocurrency analytics.

The architecture combines:

- real-time event streaming
- stream processing
- relational storage
- scheduled orchestration

The system ingests live cryptocurrency market data from the CoinGecko API, processes rolling analytics using PySpark Structured Streaming, stores processed metrics in PostgreSQL, and orchestrates daily batch analytics using Apache Airflow.

---

# High-Level Architecture

```text
CoinGecko API
        ↓
Python Kafka Producer
        ↓
Kafka Topic: crypto_prices
        ↓
PySpark Structured Streaming
        ↓
Kafka Topic: crypto_metrics
        ↓
Python PostgreSQL Consumer
        ↓
PostgreSQL: crypto_metrics table
        ↓
Apache Airflow DAG
        ↓
PostgreSQL: daily_crypto_summary table
```

---

# Architecture Layers

The project is intentionally separated into two architectural layers:

## 1. Streaming Layer

Responsible for:
- ingesting live events
- real-time processing
- rolling aggregations
- event transport

### Components

| Component | Purpose |
|---|---|
| CoinGecko API | Live cryptocurrency market data |
| Kafka Producer | Publishes raw events |
| Kafka Topic: `crypto_prices` | Raw streaming topic |
| PySpark Structured Streaming | Stream processing and aggregations |
| Kafka Topic: `crypto_metrics` | Processed metrics topic |
| PostgreSQL Consumer | Writes processed metrics to storage |

### Streaming Metrics

The Spark layer computes rolling window metrics including:

- average price
- minimum price
- maximum price
- volatility (standard deviation)

using Spark windowed aggregations.

---

## 2. Batch Orchestration Layer

Responsible for:
- scheduled analytics
- ETL orchestration
- aggregated reporting

### Components

| Component | Purpose |
|---|---|
| Apache Airflow | Workflow orchestration |
| `daily_crypto_summary` DAG | Daily ETL pipeline |
| PostgreSQL | Batch analytics storage |

### Airflow Workflow

The Airflow DAG performs:

```text
extract
→ transform
→ load
```

steps on top of the streaming output already stored in PostgreSQL.

The DAG:
1. extracts metrics from `crypto_metrics`
2. aggregates daily summaries
3. loads results into `daily_crypto_summary`

---

# Why Kafka?

Kafka is used as the event streaming backbone because it provides:

- decoupled producers and consumers
- durable event storage
- scalable streaming transport
- asynchronous communication

This allows each service to operate independently.

---

# Why Spark Structured Streaming?

Spark Structured Streaming replaces manual Python state management and enables:

- scalable windowed aggregations
- streaming transformations
- fault-tolerant checkpointing
- event-time processing

instead of maintaining rolling metrics manually in Python.

---

# Why PostgreSQL?

PostgreSQL is used for structured analytical storage because it provides:

- relational querying
- persistence
- compatibility with Airflow ETL workflows
- analytical aggregation support

---

# Why Airflow?

Airflow is used to orchestrate scheduled analytical workflows on top of the streaming output stored in PostgreSQL.

The orchestration layer is intentionally separated from the real-time streaming layer, allowing:
- independent execution of streaming and batch workloads
- modular pipeline design
- downstream analytical aggregation and reporting

In this architecture, Airflow operates on persisted streaming data rather than orchestrating the streaming services themselves.

---

# Current Project Structure

```text
crypto-streaming-pipeline/
│
├── airflow/
├── crypto-market-stream/
├── sql/
├── docs/
├── scripts/
└── requirements.txt
```

---

# Future Improvements

Planned extensions include:

- sentiment ingestion pipeline
- monitoring and logging
- Docker Compose full-stack deployment
- dashboards/visualization
- cloud deployment
- schema registry integration
- Airflow sensors and hooks
