-- ============================================
-- YouTube Sentiment Streaming Tables
-- ============================================

-- ============================================
-- YouTube Sentiment Metrics Table
-- ============================================

CREATE TABLE IF NOT EXISTS youtube_sentiment_metrics (
    id SERIAL PRIMARY KEY,

    comment_id TEXT UNIQUE NOT NULL,

    platform TEXT,
    content_type TEXT,

    video_id TEXT,
    video_title TEXT,

    channel_title TEXT,
    author TEXT,
    source_query TEXT,

    comment_text TEXT,
    like_count INTEGER,

    published_at TIMESTAMP,
    ingested_at TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    language TEXT,

    sentiment_score DOUBLE PRECISION,
    sentiment_label TEXT,

    engagement_score DOUBLE PRECISION,
    weighted_sentiment_score DOUBLE PRECISION
);

-- ============================================
-- Daily Aggregated Sentiment Summary Table
-- ============================================

CREATE TABLE IF NOT EXISTS daily_youtube_sentiment_summary (
    id SERIAL PRIMARY KEY,

    summary_date DATE,

    source_query TEXT,
    channel_title TEXT,
    sentiment_label TEXT,

    comment_count INTEGER,

    avg_sentiment_score DOUBLE PRECISION,
    avg_weighted_sentiment_score DOUBLE PRECISION,
    avg_engagement_score DOUBLE PRECISION,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Useful Indexes
-- ============================================

CREATE INDEX IF NOT EXISTS idx_youtube_comment_id
ON youtube_sentiment_metrics(comment_id);

CREATE INDEX IF NOT EXISTS idx_youtube_processed_at
ON youtube_sentiment_metrics(processed_at);

CREATE INDEX IF NOT EXISTS idx_youtube_published_at
ON youtube_sentiment_metrics(published_at);

CREATE INDEX IF NOT EXISTS idx_youtube_channel
ON youtube_sentiment_metrics(channel_title);

CREATE INDEX IF NOT EXISTS idx_youtube_sentiment_label
ON youtube_sentiment_metrics(sentiment_label);

CREATE INDEX IF NOT EXISTS idx_youtube_source_query
ON youtube_sentiment_metrics(source_query);

CREATE INDEX IF NOT EXISTS idx_youtube_language
ON youtube_sentiment_metrics(language);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date
ON daily_youtube_sentiment_summary(summary_date);

CREATE INDEX IF NOT EXISTS idx_daily_summary_channel
ON daily_youtube_sentiment_summary(channel_title);

CREATE INDEX IF NOT EXISTS idx_daily_summary_source_query
ON daily_youtube_sentiment_summary(source_query);

CREATE INDEX IF NOT EXISTS idx_daily_summary_sentiment_label
ON daily_youtube_sentiment_summary(sentiment_label);

-- ============================================
-- Validation Queries
-- ============================================

-- Check row count
-- SELECT COUNT(*) FROM youtube_sentiment_metrics;

-- Check sentiment distribution
-- SELECT sentiment_label, COUNT(*)
-- FROM youtube_sentiment_metrics
-- GROUP BY sentiment_label;

-- Check daily aggregation
-- SELECT summary_date, COUNT(*)
-- FROM daily_youtube_sentiment_summary
-- GROUP BY summary_date
-- ORDER BY summary_date;
