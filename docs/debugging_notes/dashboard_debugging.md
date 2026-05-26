# Dashboard Debugging Notes

## Overview

This document summarises several debugging challenges encountered while building the Looker Studio dashboard layer for the cryptocurrency analytics and sentiment monitoring platform.

The primary issues involved:

- SSL connectivity
- dashboard blending behaviour
- aggregation mismatches
- sparse event data
- chart interpretability
- dashboard filtering behaviour
- analytical grain alignment

Although the dashboards ultimately became the presentation layer of the project, significant engineering work was required to make the visualisations stable, interpretable, and analytically meaningful.

---

## Initial Goal

The dashboard layer was designed to visualise:

- cryptocurrency market analytics
- social sentiment monitoring
- market and sentiment correlation behaviour

The dashboards consumed analytical warehouse views from the Neon PostgreSQL warehouse using Looker Studio.

The intended analytical flow was:

```text
Streaming Pipelines
        ↓
Operational PostgreSQL
        ↓
Airflow Warehouse Loading
        ↓
Neon Warehouse
        ↓
Dashboard Views
        ↓
Looker Studio
```

Several issues emerged during implementation.

---

# Issue 1 — Looker Studio SSL Connection Failure

## Problem

Initial attempts to connect Looker Studio directly to Neon PostgreSQL failed.

The dashboard connection produced SSL-related certificate errors.

---

## Root Cause

Neon PostgreSQL requires secure SSL connections.

Looker Studio could not validate the SSL certificate chain automatically during the initial connection attempts.

---

## Solution

The issue was resolved by enabling the required SSL certificate trust configuration.

The dashboard connection was successfully established after accepting the required secure connection settings.

This allowed Looker Studio to query the Neon warehouse directly.

---

# Issue 2 — Dashboard Blending Produced Incorrect Charts

## Problem

Several blended charts initially produced:

- duplicated rows
- exploding values
- distorted scaling
- visually incorrect line charts
- unstable aggregations

In some cases, charts displayed only a single point instead of historical trends.

---

## Root Cause

The dashboard attempted to blend datasets with incompatible analytical grains.

The cryptocurrency dataset contained dense daily historical observations, while the sentiment dataset contained sparse event-driven observations.

Joining these datasets directly inside Looker Studio created one-to-many blending behaviour.

---

## Solution

The issue was resolved by introducing aggregated warehouse summary views.

Rather than blending raw event tables directly inside the dashboard, the warehouse layer pre-aggregated sentiment data into compatible daily analytical views.

This simplified dashboard joins and stabilised visual behaviour.

---

# Issue 3 — Sentiment Data Appeared on a Single Day

## Problem

The correlation dashboard initially displayed all sentiment data concentrated on a single date.

This made the dashboard appear analytically incorrect despite successful ingestion.

---

## Root Cause

The warehouse aggregation logic originally grouped sentiment events using:

```sql
processed_at
```

instead of:

```sql
published_at
```

Since all comments were processed during a short ingestion window, the dashboard collapsed all sentiment activity into a single processing day.

---

## Solution

The aggregation logic was updated to use:

```sql
DATE(published_at)
```

This aligned sentiment events with the original publication date of each YouTube comment.

The resulting dashboard correctly distributed sentiment activity across multiple historical dates.

---

# Issue 4 — Sparse Sentiment Data Reduced Correlation Density

## Problem

The sentiment dataset was significantly smaller than the cryptocurrency price dataset.

As a result:

- correlation charts appeared sparse
- sentiment points were intermittent
- some time windows contained no sentiment observations

---

## Root Cause

The sentiment ingestion pipeline was running locally for relatively short collection windows.

Unlike cryptocurrency market APIs, sentiment ingestion required continuous active collection time.

Local infrastructure limitations therefore constrained:

- ingestion duration
- collection scale
- historical coverage

---

## Solution

Rather than artificially inflating the dataset, the limitation was documented transparently.

The dashboard was reframed as:

- an exploratory analytical tool
- an architectural proof of concept
- a streaming integration demonstration

rather than a statistically rigorous predictive system.

This produced a more technically honest presentation.

---

# Issue 5 — Correlation Visualisation Was Difficult to Interpret

## Problem

The combined market and sentiment dashboard initially appeared visually confusing.

Overlaying:

- dense price series
- sparse sentiment events

created charts that were difficult to read.

---

## Root Cause

The two datasets operated at very different event frequencies.

The market stream contained continuous historical time-series data, while sentiment events appeared irregularly.

Direct visual overlay reduced readability.

---

## Solution

Several dashboard adjustments improved interpretability:

- simplified chart layouts
- reduced visual clutter
- separated metrics more clearly
- aggregated sentiment at daily granularity
- improved axis scaling
- refined dashboard spacing

The final dashboard prioritised interpretability over excessive visual complexity.

---

# Issue 6 — Dashboard Filters Initially Failed to Update All Charts

## Problem

Early versions of the cryptocurrency dashboard filters did not propagate correctly across all visualisations.

Some charts updated dynamically while others remained static.

---

## Root Cause

Certain charts were connected to different blended data sources rather than the shared dashboard dataset.

This prevented dashboard-level controls from propagating consistently.

---

## Solution

The dashboard data sources were standardised so that connected visualisations shared compatible filtering behaviour.

This allowed dashboard controls to dynamically update:

- KPI cards
- price charts
- market capitalisation charts
- trading volume charts

simultaneously.

The resulting dashboard behaved much more like a cohesive analytical interface.

---

# Issue 7 — Dashboard Layout and Visual Density

## Problem

Initial dashboard layouts appeared visually cluttered.

Charts competed for attention and analytical flow felt inconsistent.

---

## Root Cause

The first dashboard versions prioritised adding visualisations rather than organising analytical hierarchy.

This reduced readability and interpretability.

---

## Solution

The dashboard layout was redesigned using:

- cleaner spacing
- reduced visual clutter
- grouped analytical sections
- simplified colour usage
- dark-theme styling
- more intentional chart placement

The final design prioritised:

- readability
- analytical flow
- high information density
- consistent layout behaviour

---

# Lessons Learned

Several important analytical engineering lessons emerged during dashboard development.

---

## Dashboard Tools Are Sensitive to Analytical Grain

Combining datasets with incompatible aggregation levels can quickly destabilise dashboard behaviour.

Shared analytical grain became essential for:

- stable blending
- interpretable visualisation
- dashboard filtering
- consistent aggregation

---

## Warehouse-Level Aggregation Simplifies Dashboards

Complex transformations are often more reliable inside the warehouse layer than inside BI tools.

Pre-aggregating warehouse views simplified:

- dashboard queries
- chart stability
- filter propagation
- visual consistency

---

## Event Time Is Critical for Historical Analytics

Using incorrect timestamps can completely distort historical analysis.

The distinction between:

```text
processed_at
vs
published_at
```

became especially important for correlation analysis.

---

## Honest Limitations Improve Technical Credibility

The project intentionally documents:

- sparse sentiment data
- local infrastructure limitations
- exploratory analytical scope

rather than overstating predictive significance.

This resulted in a more realistic and technically grounded presentation.

---

# Final Outcome

After resolving these issues, the final dashboard layer successfully supported:

- cryptocurrency market analytics
- sentiment monitoring
- cross-domain analytical integration
- interactive filtering
- historical trend analysis
- event-time alignment
- warehouse-driven aggregation
- dashboard visualisation

The debugging process became a valuable part of understanding real-world BI engineering, analytical modelling, and dashboard integration challenges.
