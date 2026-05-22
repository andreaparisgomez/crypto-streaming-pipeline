# Sentiment Stream Debugging Notes

## Overview

This document records the main debugging steps, design decisions, and issues encountered while building the sentiment streaming extension of the crypto streaming pipeline.

The sentiment pipeline uses YouTube comments as a real-time social sentiment source.

```text
YouTube Data API
→ Python Kafka Producer
→ Kafka Topic: youtube_raw_comments
→ PySpark Structured Streaming + VADER
→ Kafka Topic: youtube_sentiment_metrics
→ Python PostgreSQL Consumer
→ PostgreSQL: youtube_sentiment_metrics
```

---

## 1. Reddit API Access Issue

The original plan was to build a Reddit sentiment ingestion layer.

However, Reddit API access required approval through the Reddit Data API request process. The first request was rejected due to insufficient compliance/detail. A second, more detailed request was submitted with:

- educational/non-commercial purpose
- low-volume public data access
- no posting/commenting/voting/messaging
- no user profiling
- no redistribution of raw Reddit content
- GitHub repository link
- explanation of why Devvit was not suitable

Because access was not immediately available, the sentiment architecture was made source-agnostic and YouTube was selected as an alternative social sentiment source.

---

## 2. Kafka Topic Setup

Created a new raw sentiment topic for YouTube comments:

```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic youtube_raw_comments \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Created a processed sentiment topic:

```bash
docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic youtube_sentiment_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

These mirror the existing crypto topics:

```text
crypto_prices
crypto_metrics
```

---

## 3. YouTube API Setup

YouTube Data API access was configured through Google Cloud.

Steps:

1. Created/select Google Cloud project
2. Enabled YouTube Data API v3
3. Created API key
4. Restricted API key to YouTube Data API v3
5. Added key to `.env`

```env
YOUTUBE_API_KEY=your_youtube_api_key
```

This was significantly easier than Reddit API access and worked immediately for public video/comment data.

---

## 4. API Endpoint Testing

Two test scripts were created before building the full producer.

### `youtube_api_test.py`

Used to validate video search:

```text
YouTube search endpoint
→ video IDs
→ channel titles
→ video titles
```

### `youtube_comments_test.py`

Used to validate comment extraction:

```text
commentThreads endpoint
→ comment IDs
→ authors
→ comment text
→ like counts
→ publish timestamps
```

This confirmed that YouTube comments could be retrieved successfully and used as raw sentiment input.

---

## 5. Producer Development

The YouTube producer was built in stages.

### Initial Version

The first version used one hardcoded video ID.

This proved:

```text
YouTube API
→ Python
→ Kafka topic: youtube_raw_comments
```

### Dynamic Version

The producer was then extended to:

1. search for crypto-related videos using search queries
2. fetch comments for each video
3. build structured comment events
4. send each event to Kafka

Search queries used:

```python
SEARCH_QUERIES = [
    "bitcoin crypto",
    "ethereum crypto",
    "altcoin news"
]
```

This changed the pipeline from:

```text
single fixed video
```

to:

```text
dynamic multi-video ingestion
```

---

## 6. Duplicate Comment Problem

Because the producer polls YouTube repeatedly, the same comments can appear across multiple fetch cycles.

Problem:

```text
same comment_id
different ingested_at
duplicate Kafka events
```

Solution:

Producer-side deduplication was added using:

```python
seen_comment_ids = set()
```

Before sending to Kafka:

```python
if event["comment_id"] not in seen_comment_ids:
    producer.send(...)
    seen_comment_ids.add(event["comment_id"])
```

This reduces duplicate events before they enter Kafka.

A second deduplication layer was added in PostgreSQL using:

```sql
comment_id TEXT PRIMARY KEY
```

and:

```sql
ON CONFLICT (comment_id) DO NOTHING
```

This makes the database insert process idempotent.

---

## 7. Disabled Comments Issue

Some YouTube videos returned a `403` error because comments were disabled.

Error reason:

```text
commentsDisabled
```

The producer was updated to handle this gracefully:

```python
if error_reason == "commentsDisabled":
    print(f"Skipping video {video_id}: comments disabled")
    return []
```

A skip cache was also added:

```python
skipped_video_ids = set()
```

This prevents repeatedly requesting comments from videos known to have disabled comments.

---

## 8. Spark Sentiment Processor

The Spark processor reads from:

```text
youtube_raw_comments
```

and writes to:

```text
youtube_sentiment_metrics
```

The processor performs:

- JSON parsing from Kafka
- language detection
- English-only filtering
- VADER sentiment scoring
- sentiment label assignment
- engagement scoring
- weighted sentiment calculation
- writing enriched events back to Kafka

---

## 9. VADER Sentiment Analysis

The original prototype used simple keyword-based sentiment scoring.

This was replaced with VADER because YouTube/crypto comments contain informal language, emojis, slang, and emotional expressions.

VADER returns a compound score:

```text
-1 = very negative
 0 = neutral
+1 = very positive
```

Sentiment labels are assigned as:

```python
if score >= 0.05:
    return "positive"
elif score <= -0.05:
    return "negative"
else:
    return "neutral"
```

---

## 10. Multilingual Comments

During testing, some comments appeared in German and other languages.

Because VADER is mainly suitable for English social media text, a language detection step was added using `langdetect`.

The Spark processor now adds:

```text
language
```

and filters:

```python
.filter(col("language") == "en")
```

This keeps the v1 sentiment analysis English-only.

---

## 11. Engagement and Weighted Sentiment

YouTube comments include:

```text
like_count
```

The Spark processor calculates:

```python
engagement_score = log1p(like_count)
```

and:

```python
weighted_sentiment_score = sentiment_score * engagement_score
```

This allows future analytics to distinguish between low-engagement and high-engagement sentiment.

In early data, many comments had `like_count = 0`, so weighted sentiment often evaluated to zero. This is expected and may be improved later using video-level metadata such as views, likes, or comment count.

---

## 12. PostgreSQL Persistence

A PostgreSQL table was created:

```sql
CREATE TABLE youtube_sentiment_metrics (
    comment_id TEXT PRIMARY KEY,
    platform TEXT,
    content_type TEXT,
    video_id TEXT,
    video_title TEXT,
    channel_title TEXT,
    author TEXT,
    comment_text TEXT,
    like_count INT,
    published_at TIMESTAMP,
    ingested_at TIMESTAMP,
    source_query TEXT,
    language TEXT,
    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT,
    engagement_score DOUBLE PRECISION,
    weighted_sentiment_score DOUBLE PRECISION,
    processed_at TIMESTAMP
);
```

A Python Kafka consumer was created to consume from:

```text
youtube_sentiment_metrics
```

and insert into PostgreSQL.

The consumer uses:

```sql
ON CONFLICT (comment_id) DO NOTHING
```

to prevent duplicate rows.

---

## 13. Final Working Sentiment Flow

The final working sentiment pipeline:

```text
YouTube Data API
→ youtube_producer.py
→ Kafka: youtube_raw_comments
→ youtube_sentiment_spark_processor.py
→ Kafka: youtube_sentiment_metrics
→ youtube_postgres_consumer.py
→ PostgreSQL: youtube_sentiment_metrics
```

This completed the first full end-to-end social sentiment streaming pipeline.

---

## 14. Key Lessons

Main debugging and architecture lessons:

- External APIs introduce access, quota, and governance issues.
- Reddit API access is currently restrictive and approval-based.
- YouTube Data API is easier to access but produces noisy data.
- Streaming pipelines must handle duplicate events.
- Producer-level deduplication reduces noise but is not enough alone.
- Database constraints provide stronger final deduplication.
- Real social data contains spam, multilingual text, emojis, and irrelevant comments.
- Sentiment models must match the text domain.
- VADER is a good lightweight choice for English social-media-style comments.
- Spark is the right place for analytical enrichment and filtering.
- PostgreSQL is the right place for durable queryable analytics storage.

---

## Future Improvements

Planned improvements:

- spam-like comment detection
- bot-pattern detection
- video-level metadata ingestion
- comment relevance filtering
- coin/entity extraction from comments
- Airflow DAG for hourly/daily sentiment aggregation
- warehouse table combining price and sentiment windows
- dashboard-ready sentiment summary tables
