# Warehouse Design

## Overview

As the project evolved from a real-time cryptocurrency streaming pipeline into a broader analytics platform, a dedicated warehouse layer was introduced to support historical analysis, dimensional modelling, dashboard reporting, and analytical querying.

The operational PostgreSQL database (`crypto_db`) is responsible for ingestion, stream processing outputs, and intermediate persistence. Whilst this structure is well suited to transactional and near real-time workloads, it is not optimised for analytical reporting.

To support business intelligence and historical analysis, a separate cloud-based PostgreSQL warehouse was implemented using Neon.

The warehouse layer provides:

* historical trend analysis
* dimensional modelling
* aggregated analytical querying
* dashboard optimisation
* separation of operational and analytical workloads
* scalable reporting architecture

---

## Operational vs Warehouse Architecture

The project separates operational storage from analytical storage.

### Operational Layer (OLTP)

The operational PostgreSQL database (`crypto_db`) stores data generated directly by the ingestion and processing pipelines.

Examples include:

* `crypto_metrics`
* `historical_crypto_prices`
* `youtube_sentiment_metrics`
* `daily_youtube_sentiment_summary`

This layer prioritises ingestion speed, stream persistence, and operational processing.

---

### Analytical Warehouse Layer (OLAP)

The analytical warehouse is implemented using Neon PostgreSQL and stores curated analytical datasets designed for reporting and dashboard consumption.

The warehouse prioritises:

* read-heavy analytical workloads
* historical aggregation
* dimensional querying
* dashboard performance
* simplified reporting models

The warehouse follows a simplified star schema design composed of fact and dimension tables.

---

## Warehouse Data Flow

```text
Operational Database
        ↓
Airflow ELT Pipelines
        ↓
Dimension Tables
    dim_date
    dim_source
    dim_channel
    dim_sentiment_label
        ↓
Fact Tables
    fact_crypto_price_daily
    fact_youtube_sentiment_daily
        ↓
Dashboard Views
        ↓
Looker Studio
```

---

## Warehouse Schema

### Fact Tables

Fact tables store measurable business metrics and analytical events.

---

#### fact_crypto_price_daily

Grain: One row per cryptocurrency per day.

Columns:

* `date_id`
* `coin_id`
* `avg_price`
* `min_price`
* `max_price`
* `avg_market_cap`
* `avg_volume`

Foreign keys:

* `date_id`

---

#### fact_youtube_sentiment_daily

Grain: One row per date × source × channel × sentiment label.

Columns:

* `date_id`
* `source_id`
* `channel_id`
* `sentiment_id`
* `comment_count`
* `avg_sentiment_score`
* `avg_weighted_sentiment_score`
* `avg_engagement_score`

Foreign keys:

* `date_id`
* `source_id`
* `channel_id`
* `sentiment_id`

---

### Dimension Tables

Dimension tables provide descriptive business context for analytical queries.

Separating dimensions from facts improves consistency, reduces duplication, and simplifies reporting.

---

#### dim_date

Stores reusable calendar attributes used for time-series analysis.

Columns:

* `date_id`
* `full_date`
* `year`
* `month`
* `day`
* `weekday`

---

#### dim_source

Stores the original YouTube search query used during ingestion.

Examples include:

* Bitcoin
* Ethereum
* Solana

Columns:

* `source_id`
* `source_query`

---

#### dim_channel

Stores YouTube channel metadata used for sentiment analysis.

Columns:

* `channel_id`
* `channel_title`

---

#### dim_sentiment_label

Stores sentiment classification categories.

Examples include:

* positive
* neutral
* negative

Columns:

* `sentiment_id`
* `sentiment_label`

---

## Warehouse Star Schema

![Warehouse Star Schema](../images/star_schema_warehouse.png)

The star schema separates descriptive business dimensions from measurable analytical facts, providing a clean structure for reporting, dashboard development, and historical analysis.
