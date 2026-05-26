-- =========================================
-- VIEW: CRYPTO PRICE DAILY
-- =========================================

CREATE OR REPLACE VIEW vw_crypto_price_daily AS
SELECT
    d.full_date,

    f.coin_id,

    f.avg_price,
    f.min_price,
    f.max_price,

    f.avg_market_cap,
    f.avg_volume

FROM fact_crypto_price_daily f

JOIN dim_date d
    ON f.date_id = d.date_id;

-- =========================================
-- VIEW: YOUTUBE SENTIMENT DAILY
-- =========================================

CREATE OR REPLACE VIEW vw_youtube_sentiment_daily AS
SELECT
    d.full_date,

    c.channel_title,

    s.source_query,

    l.sentiment_label,

    f.comment_count,

    f.avg_sentiment_score,
    f.avg_weighted_sentiment_score,
    f.avg_engagement_score

FROM fact_youtube_sentiment_daily f

JOIN dim_date d
    ON f.date_id = d.date_id

JOIN dim_channel c
    ON f.channel_id = c.channel_id

JOIN dim_source s
    ON f.source_id = s.source_id

JOIN dim_sentiment_label l
    ON f.sentiment_id = l.sentiment_id;

-- =========================================
-- VIEW: DAILY SENTIMENT SUMMARY
-- =========================================

CREATE OR REPLACE VIEW vw_daily_sentiment_summary AS
SELECT
    full_date,

    AVG(avg_sentiment_score) AS daily_sentiment_score,

    SUM(comment_count) AS total_comments,

    AVG(avg_engagement_score) AS daily_engagement_score

FROM vw_youtube_sentiment_daily

GROUP BY full_date

ORDER BY full_date;
