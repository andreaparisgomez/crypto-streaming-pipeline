# Monitoring Architecture

## Overview

The crypto analytics platform includes a platform-level monitoring and observability layer orchestrated with Apache Airflow.

The goal of this layer is to validate the operational health of the complete analytics stack, including:

- streaming ingestion pipelines
- Spark Structured Streaming transformations
- operational PostgreSQL persistence
- warehouse population in Neon PostgreSQL
- dashboard data readiness
- data quality validation
- pipeline freshness monitoring

This monitoring layer transforms the project from a simple data pipeline into a production-style analytics platform with operational awareness and validation.

---

# Monitoring DAG

![Platform Health Check DAG](../images/platform_health_check_airflow.png)

The monitoring system is implemented as an Airflow DAG:

```text
platform_health_check.py
```

The DAG runs hourly and validates all major platform layers.

---

# Monitoring Architecture

```text
Operational PostgreSQL Checks
            ↓
Streaming Freshness Validation
            ↓
Data Quality Validation
            ↓
Warehouse Validation
            ↓
Dashboard Readiness Validation
```

---

# Platform Components Monitored

## 1. Operational Pipeline Monitoring

### Crypto Streaming Pipeline

Checks:
- `crypto_metrics` table existence
- table population
- latest `inserted_at` timestamp freshness

Purpose:
- validate Kafka → Spark → PostgreSQL ingestion
- detect stalled streaming pipelines
- validate operational database writes

---

### YouTube Sentiment Pipeline

Checks:
- `youtube_sentiment_metrics` table existence
- table population
- latest `processed_at` freshness

Purpose:
- validate sentiment transformation completion
- confirm Spark processing stage is operational
- detect stale sentiment ingestion

---

# Data Quality Validation

## Crypto Metrics Quality Checks

The DAG validates:

- non-null:
  - coin
  - window_start
  - window_end
  - avg_price
  - min_price
  - max_price
  - inserted_at

- numerical sanity:
  - no negative prices
  - no negative volatility

- window consistency:
  - `window_end > window_start`

### Note on Volatility

`volatility` is allowed to be NULL.

This occurs naturally when a streaming aggregation window contains insufficient observations to compute standard deviation.

This was intentionally excluded from failure conditions to avoid false positives.

---

## Sentiment Metrics Quality Checks

The DAG validates:

- required metadata fields
- sentiment scores
- processed timestamps
- engagement metrics
- valid sentiment labels:
  - positive
  - neutral
  - negative

Additional checks:
- no negative engagement counts
- no incomplete processed records

---

# Warehouse Monitoring

The monitoring DAG validates the Neon analytical warehouse layer.

## Fact Tables

- `fact_crypto_price_daily`
- `fact_youtube_sentiment_daily`

## Dimension Tables

- `dim_date`
- `dim_source`
- `dim_channel`
- `dim_sentiment_label`

Checks:
- table existence
- non-empty population

Purpose:
- validate Airflow warehouse loading DAGs
- detect failed ELT operations
- validate analytical model integrity

---

# Dashboard Layer Validation

The DAG validates dashboard-facing analytical views used by Looker Studio.

Validated views:

- `vw_crypto_price_daily`
- `vw_daily_sentiment_summary`
- `vw_youtube_sentiment_daily`

Checks:
- view existence
- non-empty analytical output

Purpose:
- validate BI readiness
- detect broken analytical transformations
- ensure dashboards receive usable data

---

# Failure Detection

The monitoring DAG is designed to detect:

- stalled streaming ingestion
- failed Spark processing
- missing warehouse loads
- broken dashboard views
- invalid analytical outputs
- stale data
- schema mismatches
- null/invalid metrics

---

# Operational Debugging Lessons

Several production-style debugging challenges were encountered during implementation:

- Docker Compose environment propagation
- Airflow container environment isolation
- Neon PostgreSQL SSL connectivity
- Airflow DAG parsing issues
- Airflow 3 operator deprecations
- Spark Kafka connector compatibility
- warehouse view naming mismatches
- mathematically valid NULL volatility handling

These debugging steps were documented separately in:

```text
docs/debugging_notes/
```

---

# Future Monitoring Enhancements

Potential future upgrades include:

- Slack/email alerting
- anomaly threshold detection
- warehouse row delta monitoring
- DAG SLA monitoring
- data freshness dashboards
- automated retry escalation
- observability dashboards
- dbt test integration

---

# Summary

The monitoring layer provides production-style observability across the entire analytics platform.

It validates:

- streaming infrastructure
- data transformations
- operational persistence
- warehouse integrity
- dashboard readiness
- data quality

This monitoring architecture significantly improves platform reliability, operational awareness, and production realism.
