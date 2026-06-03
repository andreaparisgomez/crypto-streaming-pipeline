-- =========================================
-- DIMENSION TABLES
-- =========================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    weekday TEXT
);

CREATE TABLE IF NOT EXISTS dim_source (
    source_id SERIAL PRIMARY KEY,
    source_query TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id SERIAL PRIMARY KEY,
    channel_title TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_sentiment_label (
    sentiment_id SERIAL PRIMARY KEY,
    sentiment_label TEXT UNIQUE NOT NULL
);

-- =========================================
-- FACT TABLE: CRYPTO DAILY PRICES
-- =========================================

CREATE TABLE IF NOT EXISTS fact_crypto_price_daily (
    fact_id SERIAL PRIMARY KEY,

    date_id INTEGER REFERENCES dim_date(date_id),

    coin_id TEXT NOT NULL,

    avg_price NUMERIC,
    min_price NUMERIC,
    max_price NUMERIC,

    avg_market_cap NUMERIC,
    avg_volume NUMERIC,

    UNIQUE(date_id, coin_id)
);

-- =========================================
-- FACT TABLE: YOUTUBE SENTIMENT DAILY
-- =========================================

CREATE TABLE IF NOT EXISTS fact_youtube_sentiment_daily (
    fact_id SERIAL PRIMARY KEY,

    date_id INTEGER REFERENCES dim_date(date_id),

    source_id INTEGER REFERENCES dim_source(source_id),

    channel_id INTEGER REFERENCES dim_channel(channel_id),

    sentiment_id INTEGER REFERENCES dim_sentiment_label(sentiment_id),

    comment_count INTEGER,

    avg_sentiment_score NUMERIC,
    avg_weighted_sentiment_score NUMERIC,
    avg_engagement_score NUMERIC,

    UNIQUE(
        date_id,
        source_id,
        channel_id,
        sentiment_id
    )
);

-- =========================================
-- INDEXES
-- =========================================

CREATE INDEX IF NOT EXISTS idx_crypto_date
ON fact_crypto_price_daily(date_id);

CREATE INDEX IF NOT EXISTS idx_crypto_coin
ON fact_crypto_price_daily(coin_id);

CREATE INDEX IF NOT EXISTS idx_sentiment_date
ON fact_youtube_sentiment_daily(date_id);

CREATE INDEX IF NOT EXISTS idx_sentiment_channel
ON fact_youtube_sentiment_daily(channel_id);

CREATE INDEX IF NOT EXISTS idx_sentiment_source
ON fact_youtube_sentiment_daily(source_id);

CREATE INDEX IF NOT EXISTS idx_sentiment_label
ON fact_youtube_sentiment_daily(sentiment_id);
