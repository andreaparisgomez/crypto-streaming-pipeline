# Crypto Streaming Pipeline — Architecture

## Overview

This project is a hybrid real-time and batch data engineering platform for cryptocurrency analytics and social sentiment monitoring.

The architecture combines:

* real-time event streaming
* distributed stream processing
* operational relational storage
* cloud analytical warehousing
* scheduled orchestration
* dashboard visualisation

The platform ingests live cryptocurrency market data from the CoinGecko API alongside cryptocurrency-related YouTube comments, processes streaming analytics using PySpark Structured Streaming, persists operational data in PostgreSQL, orchestrates warehouse workflows using Apache Airflow, and exposes analytical insights through Looker Studio dashboards.

---

## Platform Architecture Diagram

![Platform Architecture](../images/platform_architecture_diagram.png)

---

## Architecture Layers

The platform is intentionally separated into several architectural layers.

### 1. Streaming Layer

Responsible for:

* real-time event ingestion
* stream transport
* distributed processing
* rolling aggregations
* asynchronous communication

The streaming layer is built around Apache Kafka and PySpark Structured Streaming.

#### Core Components

| Component                    | Purpose                                      |
| ---------------------------- | -------------------------------------------- |
| CoinGecko API                | Live cryptocurrency market data              |
| YouTube Data API             | Cryptocurrency-related social sentiment data |
| Kafka Producers              | Publish raw streaming events                 |
| Kafka Topics                 | Decoupled event transport                    |
| PySpark Structured Streaming | Real-time processing and transformations     |
| Python PostgreSQL Consumers  | Persist processed streaming outputs          |

---

### 2. Operational Storage Layer

Responsible for:

* intermediate persistence
* streaming output storage
* near real-time operational querying

The operational PostgreSQL database (`crypto_db`) stores processed outputs from the streaming layer before downstream analytical transformation.

Examples include:

* `crypto_metrics`
* `historical_crypto_prices`
* `youtube_sentiment_metrics`

In addition, Airflow-generated analytical summary tables such as `daily_youtube_sentiment_summary` are maintained within the operational database.

This layer prioritises ingestion and persistence rather than analytical querying.

---

### 3. Orchestration Layer

Responsible for:

* scheduled warehouse workflows
* analytical aggregation
* monitoring and health checks
* warehouse loading automation

Apache Airflow orchestrates workflows independently from the streaming infrastructure.

Key responsibilities include:

* loading warehouse fact tables
* aggregating daily summaries
* warehouse validation
* monitoring pipeline behaviour
* orchestrating analytical refresh workflows

This separation between streaming and orchestration reflects common modern data platform design patterns.

---

### 4. Analytical Warehouse Layer

Responsible for:

* historical analytical storage
* dimensional modelling
* dashboard querying
* aggregated reporting

The analytical warehouse is implemented using Neon PostgreSQL.

The warehouse follows a simplified star schema design composed of:

* fact tables
* dimension tables
* analytical views

The warehouse is populated through ELT workflows and exposed to downstream dashboards through curated analytical views.

Examples include:

* `fact_crypto_price_daily`
* `fact_youtube_sentiment_daily`
* `dim_date`
* `dim_source`
* `dim_channel`
* `dim_sentiment_label`

Further warehouse design details are documented in `warehouse_design.md`.

---

### 5. Dashboard and Analytics Layer

Responsible for:

* business-oriented visualisation
* historical trend analysis
* sentiment analytics
* market correlation analysis

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

The Spark processing layer computes rolling analytical metrics for each cryptocurrency over a one-minute aggregation window, including:

* Average price (`avg_price`)
* Minimum price (`min_price`)
* Maximum price (`max_price`)
* Volatility (`stddev(price_usd)`), calculated as the standard deviation of observed prices within the aggregation window
* Market capitalisation (`market_cap`), provided directly by the CoinGecko API
* Trading volume (`volume`), representing the reported 24-hour trading volume provided by the CoinGecko API

### Note on Volatility

The current volatility metric measures short-term price dispersion within a Spark aggregation window rather than the standard financial definition of volatility based on asset returns.

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

The sentiment stream operates independently from the market data stream whilst sharing downstream warehousing and orchestration infrastructure.

---

## Sentiment Processing

The Spark sentiment processor performs:

* Kafka JSON parsing
* multilingual filtering
* sentiment scoring using VADER
* sentiment label classification
* engagement scoring
* weighted sentiment calculations
* metadata enrichment

Processed events are written back into Kafka before persistence into PostgreSQL.

---

## Monitoring and Health Checks

The platform includes operational monitoring workflows implemented with Apache Airflow.

Monitoring workflows validate:

* streaming freshness
* data quality
* warehouse population
* dashboard readiness
* analytical integrity

The monitoring layer is designed to detect operational issues across the platform before they impact downstream analytics and reporting.

Further monitoring details are documented in `monitoring_architecture.md`.

---

## Technology Rationale

### Why Kafka?

Kafka provides:

* decoupled event transport
* durable message persistence
* asynchronous communication
* scalable streaming infrastructure

This allows producers, processors and consumers to operate independently.

---

### Why Spark Structured Streaming?

Spark Structured Streaming enables:

* distributed stream processing
* rolling aggregations
* fault-tolerant checkpointing
* event-time processing
* scalable streaming transformations

This removes the need for manual state management and supports production-oriented streaming workflows.

---

### Why PostgreSQL?

PostgreSQL provides:

* reliable relational persistence
* analytical querying support
* compatibility with Airflow workflows
* structured intermediate storage

The operational database acts as persistence between streaming infrastructure and downstream analytics.

---

### Why Airflow?

Airflow orchestrates:

* warehouse loading
* analytical aggregation
* monitoring workflows
* scheduled refresh operations

The orchestration layer operates independently from the streaming infrastructure, enabling clear separation between real-time processing and scheduled analytical workloads.

---

### Why Neon?

Neon provides:

* cloud-hosted PostgreSQL warehousing
* scalable analytical querying
* separation between operational and analytical workloads
* dashboard-friendly storage architecture

This enables a dedicated analytical layer separate from operational streaming persistence.

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

* expanded monitoring and observability
* additional social sentiment sources
* cloud deployment
* automated alerting
* larger-scale historical sentiment ingestion
* warehouse validation automation
* lower-latency dashboard refresh intervals
* dbt transformation layers
* CI/CD workflows
* distributed infrastructure deployment

---

## Conclusion

The platform evolved from a simple cryptocurrency streaming pipeline into a broader analytics architecture combining:

* distributed event streaming
* stream processing
* operational persistence
* orchestration
* cloud warehousing
* dimensional modelling
* sentiment analytics
* dashboard reporting

The resulting platform demonstrates how streaming, warehousing, orchestration, monitoring and analytics can be integrated into a cohesive end-to-end data engineering architecture.
