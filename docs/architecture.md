# Crypto Streaming Pipeline — Architecture

## Overview

This project is a hybrid real-time and batch data engineering platform for cryptocurrency analytics and social sentiment monitoring.

The architecture combines:

- real-time event streaming
- distributed stream processing
- operational relational storage
- cloud analytical warehousing
- scheduled orchestration
- dashboard visualisation

The platform ingests live cryptocurrency market data from the CoinGecko API alongside cryptocurrency-related YouTube comments, processes streaming analytics using PySpark Structured Streaming, persists operational data in PostgreSQL, orchestrates warehouse workflows using Apache Airflow, and exposes analytical insights through Looker Studio dashboards.

---

## Platform Architecture Diagram

![Platform Architecture](../images/platform_architecture_diagram.png)

---

## Architecture Layers

The platform is intentionally separated into several architectural layers.

### 1. Streaming Layer

Responsible for:

- real-time event ingestion
- stream transport
- distributed processing
- rolling aggregations
- asynchronous communication

The streaming layer is built around Apache Kafka and PySpark Structured Streaming.

#### Core Components

| Component | Purpose |
|---|---|
| CoinGecko API | Live cryptocurrency market data |
| YouTube Data API | Cryptocurrency-related social sentiment data |
| Kafka Producers | Publish raw streaming events |
| Kafka Topics | Decoupled event transport |
| PySpark Structured Streaming | Real-time processing and transformations |
| Python PostgreSQL Consumers | Persist processed streaming outputs |

---

### 2. Operational Storage Layer

Responsible for:

- intermediate persistence
- streaming output storage
- near real-time operational querying

The operational PostgreSQL database (`crypto_db`) stores processed outputs from the streaming layer before downstream analytical transformation.

Examples include:

- `crypto_metrics`
- `historical_crypto_prices`
- `youtube_sentiment_metrics`
- `daily_youtube_sentiment_summary`

This layer prioritises ingestion and persistence rather than analytical querying.

---

### 3. Orchestration Layer

Responsible for:

- scheduled warehouse workflows
- analytical aggregation
- monitoring and health checks
- warehouse loading automation

Apache Airflow orchestrates workflows independently from the streaming infrastructure.

Key responsibilities include:

- loading warehouse fact tables
- aggregating daily summaries
- warehouse validation
- monitoring pipeline behaviour
- orchestrating analytical refresh workflows

This separation between streaming and orchestration reflects common modern data platform design patterns.

---

### 4. Analytical Warehouse Layer

Responsible for:

- historical analytical storage
- dimensional modelling
- dashboard querying
- aggregated reporting

The analytical warehouse is implemented using Neon PostgreSQL.

The warehouse follows a simplified star schema design composed of:

- fact tables
- dimension tables
- analytical views

Examples include:

- `fact_crypto_price_daily`
- `fact_youtube_sentiment_daily`
- `dim_date`
- `dim_source`
- `dim_channel`
- `dim_sentiment_label`

Further warehouse design details are documented in `warehouse_design.md`.

---

### 5. Dashboard and Analytics Layer

Responsible for:

- business-oriented visualisation
- historical trend analysis
- sentiment analytics
- market correlation analysis

Looker Studio consumes analytical warehouse views exposed from the Neon warehouse layer.

The dashboard includes:

1. Cryptocurrency market analytics
2. Social sentiment monitoring
3. Market sentiment correlation analysis

Further dashboard details are documented in `dashboard_walkthrough.md`.

---

## Cryptocurrency Streaming Pipeline

The cryptocurrency stream ingests live market data from the CoinGecko API and processes rolling market analytics using Spark Structured Streaming.

### Pipeline Flow

```text
CoinGecko API
    ↓
Kafka Producer
    ↓
Kafka Topic: crypto_prices
    ↓
PySpark Structured Streaming
    ↓
Kafka Topic: crypto_metrics
    ↓
Python PostgreSQL Consumer
    ↓
PostgreSQL Operational Database
```

### Streaming Metrics

The Spark processing layer computes rolling analytical metrics including:

- average price
- minimum price
- maximum price
- volatility calculations
- market capitalisation
- trading volume

These processed metrics are persisted into PostgreSQL for downstream warehousing and analytics.

---

## Sentiment Streaming Pipeline

The platform includes a parallel social sentiment ingestion pipeline using YouTube comments related to cryptocurrency topics.

### Pipeline Flow

```text
YouTube Data API
    ↓
YouTube Producer
    ↓
Kafka Topic: youtube_raw_comments
    ↓
PySpark Sentiment Processor
    ↓
Kafka Topic: youtube_sentiment_metrics
    ↓
Python PostgreSQL Consumer
    ↓
PostgreSQL Operational Database
```

The sentiment stream operates independently from the market data stream while sharing downstream warehousing and orchestration infrastructure.

---

## Sentiment Processing

The Spark sentiment processor performs:

- Kafka JSON parsing
- multilingual filtering
- sentiment scoring using VADER
- sentiment label classification
- engagement scoring
- weighted sentiment calculations
- metadata enrichment

Processed events are written back into Kafka before persistence into PostgreSQL.

---

## Monitoring and Health Checks

The platform includes operational monitoring workflows implemented with Apache Airflow.

The monitoring layer validates:

- infrastructure availability
- warehouse loading behaviour
- streaming activity
- table freshness
- row growth behaviour

Monitoring DAGs currently include behavioural checks to confirm that streaming tables continue receiving new events over time rather than simply verifying table existence.

The monitoring layer is intentionally designed to expand further as the platform evolves.

---

## Technology Rationale

### Why Kafka?

Kafka provides:

- decoupled producers and consumers
- durable event transport
- asynchronous communication
- scalable streaming infrastructure

This allows independent services to operate without tight coupling.

---

### Why Spark Structured Streaming?

Spark Structured Streaming enables:

- distributed stream processing
- rolling aggregations
- fault-tolerant checkpointing
- event-time processing
- scalable streaming transformations

This replaces manual state management and supports more production-oriented streaming workflows.

---

### Why PostgreSQL?

PostgreSQL provides:

- reliable relational persistence
- analytical querying support
- compatibility with Airflow workflows
- structured intermediate storage

The operational database acts as persistence between streaming infrastructure and downstream analytics.

---

### Why Airflow?

Airflow orchestrates:

- scheduled analytical workflows
- warehouse loading
- monitoring tasks
- historical aggregation

The orchestration layer operates independently from the streaming infrastructure, enabling clearer separation between real-time processing and scheduled analytical workflows.

---

### Why Neon?

Neon provides:

- cloud-hosted PostgreSQL warehousing
- scalable analytical querying
- separation between operational and analytical workloads
- dashboard-friendly storage architecture

This allows the platform to maintain a dedicated analytical layer separate from operational streaming persistence.

---

## Current Project Structure

```text
crypto-streaming-pipeline/
│
├── airflow/
├── crypto-market-stream/
├── sql/
├── docs/
├── scripts/
├── images/
└── requirements.txt
```

---

## Future Improvements

Planned future improvements include:

- expanded monitoring and observability
- additional social sentiment sources
- cloud deployment
- automated alerting
- larger-scale historical sentiment ingestion
- warehouse validation automation
- lower-latency dashboard refresh intervals
- dbt transformation layers
- CI/CD workflows
- distributed infrastructure deployment

---

## Conclusion

The platform evolved from a simple cryptocurrency streaming pipeline into a broader analytics architecture combining:

- distributed event streaming
- stream processing
- operational persistence
- orchestration
- cloud warehousing
- dimensional modelling
- sentiment analytics
- dashboard reporting

The resulting architecture more closely resembles modern real-world analytics engineering and data platform workflows.
