# Warehouse Validation and Analytical Debugging Notes

## Overview

This document summarises debugging challenges encountered while validating the analytical warehouse layer, dashboard data consistency, and cross-source analytical integration.

The primary issues involved:

- warehouse loading validation
- analytical grain mismatches
- sparse sentiment aggregation
- event-time alignment
- dashboard query behaviour
- operational vs analytical timestamp handling

These debugging steps became important learning experiences in analytical engineering and warehouse design.

---

## Initial Goal

The warehouse layer was designed to support:

- historical analytical querying
- dashboard aggregation
- dimensional modelling
- cross-domain analytics
- sentiment and market correlation analysis

The intended analytical flow was:

```text
Streaming Pipelines
        ↓
Operational PostgreSQL
        ↓
Airflow Warehouse Loading
        ↓
Neon Analytical Warehouse
        ↓
Dashboard Views
        ↓
Looker Studio
```

As the platform evolved, several analytical consistency issues emerged.

---

# Issue 1 — Empty Sentiment Warehouse Tables

## Problem

The sentiment warehouse fact tables in Neon initially appeared empty despite the operational PostgreSQL database containing processed sentiment events.

Queries such as:

```sql
SELECT COUNT(*) 
FROM fact_youtube_sentiment_daily;
```

returned zero rows.

---

## Root Cause

The Airflow warehouse loading DAG had not successfully executed after the initial schema deployment.

The operational PostgreSQL database contained data, but the warehouse fact tables had not yet been populated.

---

## Solution

The warehouse loading DAGs were manually triggered from the Airflow UI.

Validation queries were then executed directly against Neon:

```sql
SELECT COUNT(*) 
FROM fact_youtube_sentiment_daily;
```

Successful row insertion confirmed that the warehouse loading workflow was functioning correctly.

---

# Issue 2 — Dashboard Date Grain Mismatch

## Problem

The market correlation dashboard initially produced distorted or visually incorrect charts.

Symptoms included:

- duplicated values
- incorrect line behaviour
- single-point visualisations
- exploding chart rows after joins

---

## Root Cause

The dashboard attempted to blend:

- dense cryptocurrency price data
- sparse sentiment event data

using incompatible date granularities.

The cryptocurrency stream contained continuous historical observations, while the sentiment stream contained far fewer event dates.

This created one-to-many join behaviour during dashboard blending.

---

## Solution

The solution involved introducing aggregated daily sentiment summary views.

Rather than joining raw sentiment events directly, the system aggregated sentiment metrics at the daily level before dashboard consumption.

This aligned both datasets to a shared daily analytical grain.

The resulting warehouse view significantly improved dashboard stability and interpretability.

---

# Issue 3 — Processing Time vs Event Time Confusion

## Problem

The correlation dashboard initially displayed all sentiment data on a single day.

Although many comments had been collected, the dashboard appeared to contain only one sentiment event timestamp.

---

## Root Cause

The warehouse aggregation logic originally used:

```sql
processed_at
```

instead of:

```sql
published_at
```

for daily aggregation.

Since all comments were processed during a short ingestion window, every event collapsed into the same processing date.

This produced incorrect historical sentiment alignment.

---

## Solution

The aggregation logic was updated to use:

```sql
DATE(published_at)
```

instead of:

```sql
DATE(processed_at)
```

This corrected the analytical interpretation by aligning sentiment events with the original comment publication time rather than ingestion time.

The resulting dashboard displayed sentiment data across multiple historical dates as intended.

---

# Issue 4 — Sparse Sentiment Data

## Problem

The sentiment dataset initially appeared too small relative to the cryptocurrency price history.

The dashboard therefore showed:

- sparse sentiment points
- limited historical overlap
- weak visual correlation density

---

## Root Cause

The sentiment ingestion pipeline was running locally on limited infrastructure and for relatively short collection windows.

Unlike cryptocurrency market data, which is continuously available historically through APIs, YouTube sentiment ingestion required active collection time.

The local development environment therefore constrained:

- ingestion duration
- collection scale
- event density

---

## Solution

Rather than artificially inflating the dataset, the project intentionally preserved the smaller dataset and documented the limitation transparently.

The dashboard and documentation were reframed as:

- exploratory analytical tooling
- architectural proof of concept
- streaming integration demonstration

rather than statistically conclusive financial modelling.

This resulted in a more realistic and technically honest presentation of the system.

---

# Issue 5 — Warehouse Validation Queries

## Problem

As additional warehouse tables and dashboard views were introduced, validating warehouse correctness manually became increasingly difficult.

---

## Root Cause

The warehouse layer now included:

- fact tables
- dimension tables
- aggregated views
- Airflow loading workflows
- dashboard-facing analytical views

Without validation queries, it became difficult to confirm:

- row consistency
- successful warehouse loading
- aggregation correctness
- dashboard readiness

---

## Solution

Dedicated warehouse validation SQL scripts were introduced.

These validation queries checked:

- warehouse row counts
- fact table population
- dimension table integrity
- dashboard view availability
- aggregation correctness

This improved confidence in downstream dashboard behaviour and warehouse consistency.

---

# Issue 6 — Dashboard Blending Complexity

## Problem

Looker Studio dashboard blending introduced unexpected analytical behaviour.

The dashboard layer occasionally produced:

- duplicated rows
- distorted aggregation behaviour
- inconsistent metric scaling

when blending multiple warehouse views.

---

## Root Cause

Dashboard blending can become unstable when:

- data grains differ
- joins are ambiguous
- aggregation levels mismatch
- sparse and dense datasets are merged

The issue became particularly visible when combining:

- historical price series
- intermittent sentiment events

within the same dashboard page.

---

## Solution

The analytical logic was simplified by:

- pre-aggregating warehouse views
- reducing dashboard-side transformations
- aligning daily grains before visualisation
- minimising dashboard blending complexity

This shifted analytical responsibility into the warehouse layer rather than the BI layer.

---

# Lessons Learned

Several important analytical engineering lessons emerged during the warehouse validation process.

---

## Event Time Matters

One of the most important lessons involved distinguishing between:

- processing time
- ingestion time
- event time

Using incorrect timestamps can completely distort analytical interpretation.

The distinction between:

```text
processed_at
vs
published_at
```

became especially important for historical sentiment analysis.

---

## Shared Analytical Grain Is Critical

Cross-source analytics requires datasets to share compatible analytical grains.

Attempting to directly combine:

- sparse event data
- dense time-series data

without aggregation alignment leads to unstable analytical behaviour.

---

## Warehouses Should Handle Transformations

Complex analytical transformations are often more reliable inside the warehouse layer than inside dashboard tools.

Pre-aggregated warehouse views simplified:

- dashboard behaviour
- visual consistency
- query stability
- analytical interpretability

---

## Honest Limitations Improve Credibility

The project intentionally documents infrastructure and dataset limitations rather than overstating analytical significance.

This produced a more realistic presentation of:

- streaming analytics
- sentiment integration
- warehouse modelling
- dashboard engineering

---

# Final Outcome

After resolving these issues, the warehouse layer successfully supported:

- historical cryptocurrency analytics
- sentiment aggregation
- dashboard querying
- event-time alignment
- dimensional modelling
- cross-domain analytics
- dashboard visualisation
- analytical validation workflows

The debugging process became a valuable part of understanding real-world analytical engineering challenges and significantly influenced the final warehouse design.
