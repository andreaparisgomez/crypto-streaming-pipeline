# Dashboard Walkthrough

## Overview

The dashboard layer provides a visual analytics interface for the cryptocurrency streaming and sentiment analysis platform.

The dashboards are powered by:

- Apache Kafka
- PySpark Structured Streaming
- PostgreSQL
- Apache Airflow
- Neon PostgreSQL Warehouse
- Looker Studio

The visualisation layer consumes analytical views from the Neon warehouse and exposes both market and sentiment-oriented insights through interactive dashboards.

The dashboard suite is divided into three primary analytical areas:

1. Cryptocurrency Market Analytics
2. Social Sentiment Monitoring
3. Market Sentiment Correlation Analysis

---

## Dashboard Architecture

The dashboard layer sits on top of the analytical warehouse and consumes transformed warehouse views rather than raw streaming tables.

```text
Streaming Pipelines
        ↓
PostgreSQL Operational Database
        ↓
Airflow Warehouse Loading
        ↓
Neon Analytical Warehouse
        ↓
Dashboard Views
        ↓
Looker Studio
```

This separation improves:

- dashboard query performance
- analytical consistency
- historical aggregation
- maintainability
- dimensional modelling support

---

# 1. Crypto Market Analytics

The first dashboard page focuses on cryptocurrency market behaviour and historical market trends.

![Crypto Market Dashboard](../images/1_full_page_crypto_market.png)

---

## Analytical Objectives

This dashboard is designed to provide:

- historical cryptocurrency price analysis
- comparative asset performance
- market capitalisation tracking
- trading volume monitoring
- interactive asset filtering

The page aggregates processed market metrics generated through the Spark streaming pipeline.

---

## Key Metrics

The dashboard includes KPI cards displaying:

- latest Bitcoin price
- latest Ethereum price
- latest Solana price

These metrics are dynamically refreshed from the warehouse layer.

---

## Historical Price Trends

The historical price trend visualisation displays:

- Bitcoin
- Ethereum
- Solana

across the historical collection window.

The chart allows rapid visual comparison between:

- asset volatility
- trend direction
- relative growth behaviour
- major market corrections

A logarithmic scaling approach was used to improve multi-asset readability due to large differences in asset price ranges.

---

## Market Capitalisation Analysis

The market capitalisation chart tracks relative market dominance across assets over time.

This visualisation helps identify:

- macro market shifts
- relative asset strength
- capital concentration behaviour
- broader market trend changes

---

## Trading Volume Analysis

The trading volume visualisation aggregates average trading activity across assets.

This provides a simplified comparison of relative market participation and liquidity.

---

## Interactive Filtering

The dashboard supports interactive filtering across the entire page.

Users can isolate individual assets dynamically using dashboard controls.

Example: Bitcoin-only filtering.

![Bitcoin Dashboard Filter](../images/dashboard_coin_filter_bitcoin.png)

This filtering behaviour updates all connected visualisations simultaneously, enabling focused exploratory analysis without modifying underlying queries.

---

# 2. Social Sentiment Monitoring

The second dashboard page focuses on cryptocurrency-related YouTube sentiment analytics.

![Social Sentiment Dashboard](../images/2_full_page_sentiment_monitoring.png)

---

## Analytical Objectives

This dashboard provides visibility into:

- social sentiment trends
- creator engagement
- audience behaviour
- positive vs negative sentiment distribution
- engagement-weighted sentiment patterns

The underlying data is collected from cryptocurrency-related YouTube comments and processed through the Spark sentiment pipeline.

---

## Sentiment Processing Pipeline

The sentiment analytics layer performs:

- sentiment scoring using VADER
- sentiment label classification
- engagement scoring
- weighted sentiment calculations
- metadata enrichment

These processed events are aggregated into warehouse fact tables before dashboard visualisation.

---

## Channel Sentiment Ranking

This visualisation ranks YouTube channels according to average sentiment score.

The chart enables rapid identification of:

- highly positive creators
- neutral discussion channels
- negative sentiment outliers

This creates an exploratory layer for comparing sentiment behaviour across different content creators.

---

## Comment Sentiment Distribution

The sentiment distribution pie chart displays the proportional breakdown of:

- positive comments
- neutral comments
- negative comments

This provides a high-level overview of audience sentiment across the collected dataset.

---

## Engagement Analysis

The engagement chart ranks channels by average engagement score.

This introduces an additional analytical dimension beyond simple sentiment polarity.

High-engagement content can therefore be compared against:

- sentiment direction
- audience interaction intensity
- creator influence

---

## Dashboard Interpretation Notes

The current sentiment dataset is relatively small due to local infrastructure runtime constraints and limited collection windows.

As a result:

- the dashboard is architecturally representative
- the visualisations demonstrate pipeline functionality
- the platform is designed for longer-running ingestion and larger-scale future collection

The current results should therefore be interpreted as exploratory rather than statistically conclusive.

---

# 3. Market Sentiment Correlation Analysis

The third dashboard page explores relationships between cryptocurrency price action and social sentiment behaviour.

![Market Sentiment Correlation Dashboard](../images/3_full_page_market_correlation.png)

---

## Analytical Objectives

This dashboard investigates whether changes in social sentiment coincide with or precede observable market behaviour.

The visualisation combines:

- historical Bitcoin price movement
- daily sentiment events
- sentiment polarity spikes
- temporal market context

---

## Correlation Design

The dashboard overlays:

- Bitcoin price trends
- daily sentiment score events

onto a shared timeline.

This allows users to visually inspect:

- sentiment spikes
- negative sentiment events
- potential market reactions
- temporal clustering behaviour

---

## Event-Time Alignment Challenges

One major challenge in building the correlation dashboard involved temporal alignment between:

- continuously collected market data
- intermittently collected sentiment events

The sentiment collection layer produced sparse event timestamps compared with the denser cryptocurrency price stream.

Several iterations of aggregation logic and event alignment were therefore required to make the dashboard visually interpretable.

This became an important engineering consideration in the analytical design process.

---

## Interpretation Considerations

The current dashboard should not be interpreted as evidence of causal financial prediction.

Instead, the purpose of the dashboard is to demonstrate:

- cross-domain analytical integration
- streaming data fusion
- event-time alignment
- sentiment aggregation workflows
- exploratory analytical visualisation

The dashboard primarily represents an architectural and analytical proof of concept.

---

# Dashboard Design Decisions

Several design decisions were made intentionally during dashboard development.

---

## Dark Theme

A dark visual theme was selected to:

- improve readability
- create stronger chart contrast
- align with financial analytics aesthetics
- reduce visual clutter

---

## Minimal Layout Design

The dashboards prioritise:

- high information density
- minimal decorative elements
- analytical readability
- consistent spacing
- clear chart separation

---

## Layer Separation

The dashboards consume analytical warehouse views rather than operational streaming tables directly.

This separation improves:

- maintainability
- query stability
- dashboard responsiveness
- analytical consistency

---

# Current Limitations

The current implementation has several intentional limitations.

---

## Limited Historical Sentiment Data

The sentiment pipeline currently operates using local infrastructure and intermittent collection windows.

This limits:

- historical depth
- event density
- statistical significance

---

## Dashboard Refresh Latency

Although the streaming layer updates continuously, dashboard refresh intervals are constrained by:

- warehouse refresh workflows
- Looker Studio refresh behaviour
- scheduled aggregation timing

The platform therefore behaves as a near real-time analytical system rather than a fully streaming-native dashboard environment.

---

## Limited Asset Coverage

The current implementation focuses primarily on:

- Bitcoin
- Ethereum
- Solana

Additional assets can be incorporated through future pipeline expansion.

---

# Future Improvements

Potential future dashboard improvements include:

- additional social platforms
- expanded asset coverage
- lower-latency dashboard refresh intervals
- anomaly detection visualisations
- streaming-native dashboard updates
- sentiment trend forecasting
- advanced warehouse modelling
- automated monitoring dashboards
- larger-scale historical ingestion

---

# Conclusion

The dashboard layer transforms the underlying streaming and warehousing infrastructure into a business-oriented analytical interface.

The resulting platform demonstrates:

- real-time streaming analytics
- sentiment enrichment
- dimensional warehousing
- dashboard engineering
- cross-domain analytical integration
- distributed data platform design

The dashboards ultimately serve as the presentation layer for a broader end-to-end data engineering architecture.
