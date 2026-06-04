# Analytics Design Decisions

## Overview

This document explains the reasoning behind the key analytical and engineering decisions made throughout the project.

Whilst the architecture, warehouse, dashboard and monitoring documentation describe what was built, this document focuses on why specific metrics, aggregations, modelling choices and analytical workflows were selected.

The objective was not simply to build a streaming pipeline, but to design an analytics platform capable of supporting meaningful reporting, monitoring and exploratory analysis.

---

# Cryptocurrency Market Analytics

## Why Cryptocurrency Prices?

Price is the primary measure of asset value and forms the foundation of most financial analysis.

Historical price behaviour enables:

* trend analysis
* performance comparison
* volatility monitoring
* market correlation analysis

Price data therefore serves as the core analytical metric throughout the platform.

---

## Why Market Capitalisation?

Price alone can be misleading when comparing cryptocurrencies.

For example, an asset trading at £500 is not necessarily larger or more important than an asset trading at £50.

Market capitalisation provides a measure of total network value and allows assets to be compared on a more meaningful basis.

The metric was included to support:

* market dominance analysis
* relative asset comparison
* long-term market trends

---

## Why Trading Volume?

Trading volume provides context that price alone cannot.

An increase in price accompanied by strong volume may indicate broad market participation, whereas a similar price movement on low volume may be less significant.

Volume was included to support:

* liquidity analysis
* market participation monitoring
* trend validation

---

## Why Volatility?

Price changes alone do not describe market stability.

Volatility was included to provide a measure of short-term price dispersion within each Spark aggregation window.

The current implementation calculates volatility as the standard deviation of observed prices within a streaming window.

This provides a simple indicator of market instability and highlights periods of elevated market activity.

### Design Limitation

This implementation differs from the traditional financial definition of volatility, which is normally calculated from returns rather than raw prices.

The metric should therefore be interpreted as a measure of short-term price variability rather than formal financial volatility.

---

# Streaming Aggregation Design

## Why Aggregate in Spark?

The platform could have stored every raw price observation directly in PostgreSQL.

Instead, Spark computes rolling aggregations before persistence.

This approach was chosen because analytical consumers are generally interested in summary statistics rather than individual events.

Pre-aggregating data:

* reduces storage requirements
* simplifies dashboard queries
* improves reporting performance
* shifts computational workload away from downstream systems

---

## Why One-Minute Aggregation Windows?

One-minute windows provide a balance between granularity and usability.

Shorter windows increase noise and storage volume, whilst longer windows reduce responsiveness.

The chosen window length provides:

* near real-time analytics
* manageable storage growth
* meaningful short-term trend visibility

---

# Sentiment Analytics Design

## Why Use YouTube Comments?

The project required a public source of user-generated cryptocurrency discussion.

YouTube comments provide:

* large volumes of publicly accessible text
* engagement metadata
* creator-level context
* cryptocurrency-specific discussions

This makes YouTube a useful source for exploratory sentiment analysis.

---

## Why Use VADER?

VADER was selected because it performs well on short, informal social-media-style text.

Advantages include:

* lightweight processing
* interpretable scoring
* no model training requirements
* fast execution within streaming workflows

This makes VADER well suited for real-time sentiment enrichment.

---

## Why Store Both Scores and Labels?

The platform stores:

* sentiment_score
* sentiment_label

rather than labels alone.

The numerical score preserves more information and allows aggregation, averaging and trend analysis.

Labels provide a simplified business-friendly interpretation.

Both representations therefore serve different analytical purposes.

---

## Why Weighted Sentiment?

Not all comments contribute equally to discussion.

A comment receiving hundreds of likes is likely to have greater visibility and influence than a comment receiving no engagement.

Weighted sentiment was therefore introduced to incorporate audience interaction into sentiment measurements.

This allows highly engaged comments to contribute proportionally more to aggregate sentiment calculations.

---

## Why Calculate Engagement Score?

Sentiment alone measures polarity but does not measure audience interaction.

The engagement score introduces an additional analytical dimension by capturing audience response intensity.

This supports:

* creator comparison
* audience behaviour analysis
* sentiment versus engagement exploration

---

# Warehouse Design Decisions

## Why Separate Operational and Analytical Databases?

The project intentionally separates:

* operational storage
* analytical storage

Operational databases prioritise ingestion and persistence.

Analytical databases prioritise reporting and aggregation.

Separating these workloads improves maintainability and more closely reflects modern data platform architecture.

---

## Why Use a Star Schema?

The warehouse follows a simplified star schema design consisting of fact and dimension tables.

This structure was selected because it:

* reduces duplication
* simplifies analytical queries
* improves dashboard usability
* supports dimensional analysis

Star schemas remain one of the most common modelling approaches in analytical systems.

---

## Why Use Dimension Tables?

Entities such as channels, sentiment labels and search queries are stored separately from fact tables.

This design:

* reduces redundancy
* improves consistency
* simplifies reporting
* supports future expansion

The warehouse therefore stores business context independently from measurable metrics.

---

# Dashboard Design Decisions

## Why Separate Dashboards into Three Areas?

The platform dashboard is organised into:

1. Cryptocurrency Market Analytics
2. Social Sentiment Monitoring
3. Market Sentiment Correlation Analysis

Each dashboard answers a different analytical question.

This separation improves readability and prevents unrelated metrics from competing for visual space.

---

## Why Include a Correlation Dashboard?

The project was designed to explore relationships between market behaviour and social sentiment.

The correlation dashboard combines:

* Bitcoin price movements
* sentiment events
* engagement trends

The objective is not predictive modelling, but exploratory analysis of potential relationships between social discussion and market activity.

---

# Monitoring Design Decisions

## Why Validate Freshness Instead of Only Table Existence?

A populated table does not necessarily indicate a healthy pipeline.

Data may be stale even when tables contain records.

The monitoring layer therefore validates:

* table existence
* row population
* timestamp freshness

This provides a more realistic measure of pipeline health.

---

## Why Include Data Quality Checks?

Data quality issues often propagate silently through analytical systems.

The monitoring layer validates:

* required fields
* numerical constraints
* sentiment labels
* timestamp consistency

This helps detect invalid data before it reaches dashboards and reporting workflows.

---

# Future Improvements

Several analytical improvements have been identified during development.

Examples include:

* sentiment aggregation weighted by comment counts to reduce aggregation bias
* additional sentiment sources
* more advanced NLP models
* anomaly detection
* predictive analytics
* expanded asset coverage

These improvements were intentionally deferred to keep the project focused on building a complete end-to-end analytics platform.

---

# Summary

Many of the design choices in this project were driven by analytical usefulness rather than technical implementation alone.

Metrics, aggregations, warehouse structures and dashboards were selected to support meaningful analysis whilst maintaining a practical and maintainable architecture.

The resulting platform demonstrates not only the implementation of modern data engineering tools, but also the reasoning behind the analytical workflows built on top of them.
