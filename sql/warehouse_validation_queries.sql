-- =========================================
-- TABLE ROW COUNT CHECKS
-- =========================================

SELECT COUNT(*) AS dim_date_count
FROM dim_date;

SELECT COUNT(*) AS dim_source_count
FROM dim_source;

SELECT COUNT(*) AS dim_channel_count
FROM dim_channel;

SELECT COUNT(*) AS dim_sentiment_label_count
FROM dim_sentiment_label;

SELECT COUNT(*) AS fact_crypto_price_daily_count
FROM fact_crypto_price_daily;

SELECT COUNT(*) AS fact_youtube_sentiment_daily_count
FROM fact_youtube_sentiment_daily;

-- =========================================
-- DATE COVERAGE CHECKS
-- =========================================

SELECT
    MIN(d.full_date) AS first_crypto_date,
    MAX(d.full_date) AS latest_crypto_date,
    COUNT(DISTINCT d.full_date) AS crypto_days
FROM fact_crypto_price_daily f
JOIN dim_date d
    ON f.date_id = d.date_id;

SELECT
    MIN(d.full_date) AS first_sentiment_date,
    MAX(d.full_date) AS latest_sentiment_date,
    COUNT(DISTINCT d.full_date) AS sentiment_days
FROM fact_youtube_sentiment_daily f
JOIN dim_date d
    ON f.date_id = d.date_id;

-- =========================================
-- DASHBOARD VIEW CHECKS
-- =========================================

SELECT *
FROM vw_crypto_price_daily
LIMIT 10;

SELECT *
FROM vw_youtube_sentiment_daily
LIMIT 10;

SELECT *
FROM vw_daily_sentiment_summary
LIMIT 10;

-- =========================================
-- ASSET COVERAGE CHECK
-- =========================================

SELECT
    coin_id,
    COUNT(*) AS rows,
    MIN(full_date) AS first_date,
    MAX(full_date) AS latest_date
FROM vw_crypto_price_daily
GROUP BY coin_id
ORDER BY coin_id;

-- =========================================
-- SENTIMENT COVERAGE CHECK
-- =========================================

SELECT
    sentiment_label,
    SUM(comment_count) AS total_comments,
    AVG(avg_sentiment_score) AS avg_score
FROM vw_youtube_sentiment_daily
GROUP BY sentiment_label
ORDER BY total_comments DESC;

-- =========================================
-- CHANNEL ENGAGEMENT CHECK
-- =========================================

SELECT
    channel_title,
    SUM(comment_count) AS total_comments,
    AVG(avg_engagement_score) AS avg_engagement_score
FROM vw_youtube_sentiment_daily
GROUP BY channel_title
ORDER BY avg_engagement_score DESC;
