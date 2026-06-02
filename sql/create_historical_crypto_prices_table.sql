-- ============================================
-- Historical Cryptocurrency Price Data Table
-- ============================================

CREATE TABLE IF NOT EXISTS historical_crypto_prices (
    id SERIAL PRIMARY KEY,

    coin_id TEXT NOT NULL,
    currency TEXT NOT NULL,

    price_timestamp TIMESTAMP NOT NULL,

    price_usd NUMERIC,
    market_cap NUMERIC,
    total_volume NUMERIC,

    source TEXT DEFAULT 'coingecko_historical',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT historical_crypto_prices_coin_id_currency_price_timestamp_key
        UNIQUE (coin_id, currency, price_timestamp)
);

-- ============================================
-- Useful Indexes
-- ============================================

CREATE INDEX IF NOT EXISTS idx_historical_crypto_price_timestamp
ON historical_crypto_prices(price_timestamp);

CREATE INDEX IF NOT EXISTS idx_historical_crypto_coin_id
ON historical_crypto_prices(coin_id);

CREATE INDEX IF NOT EXISTS idx_historical_crypto_coin_currency_timestamp
ON historical_crypto_prices(coin_id, currency, price_timestamp);

CREATE INDEX IF NOT EXISTS idx_historical_crypto_source
ON historical_crypto_prices(source);

-- ============================================
-- Validation Queries
-- ============================================

-- Check total row count
-- SELECT COUNT(*) FROM historical_crypto_prices;

-- Check available coins
-- SELECT DISTINCT coin_id
-- FROM historical_crypto_prices;

-- Check available currencies
-- SELECT DISTINCT currency
-- FROM historical_crypto_prices;

-- Check timestamp range
-- SELECT MIN(price_timestamp), MAX(price_timestamp)
-- FROM historical_crypto_prices;

-- Daily average prices
-- SELECT
--     DATE(price_timestamp) AS price_date,
--     coin_id,
--     AVG(price_usd) AS avg_price_usd
-- FROM historical_crypto_prices
-- GROUP BY DATE(price_timestamp), coin_id
-- ORDER BY price_date, coin_id;
