-- ============================================
-- Historical Cryptocurrency Price Data Table
-- ============================================

CREATE TABLE IF NOT EXISTS historical_crypto_prices (
    id SERIAL PRIMARY KEY,

    coin_id TEXT NOT NULL,
    symbol TEXT,
    coin_name TEXT,

    price_date DATE NOT NULL,

    current_price FLOAT,
    market_cap FLOAT,
    total_volume FLOAT,

    price_change_percentage_24h FLOAT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Useful Indexes
-- ============================================

CREATE INDEX IF NOT EXISTS idx_crypto_price_date
ON historical_crypto_prices(price_date);

CREATE INDEX IF NOT EXISTS idx_crypto_coin_id
ON historical_crypto_prices(coin_id);

CREATE INDEX IF NOT EXISTS idx_crypto_coin_date
ON historical_crypto_prices(coin_id, price_date);

-- ============================================
-- Optional Uniqueness Constraint
-- Prevent duplicate records per coin per day
-- ============================================

ALTER TABLE historical_crypto_prices
ADD CONSTRAINT unique_coin_date
UNIQUE (coin_id, price_date);

-- ============================================
-- Validation Queries
-- ============================================

-- Check total row count
-- SELECT COUNT(*) FROM historical_crypto_prices;

-- Check available coins
-- SELECT DISTINCT coin_id
-- FROM historical_crypto_prices;

-- Check date range
-- SELECT MIN(price_date), MAX(price_date)
-- FROM historical_crypto_prices;

-- Daily average prices
-- SELECT
--     price_date,
--     coin_id,
--     AVG(current_price)
-- FROM historical_crypto_prices
-- GROUP BY price_date, coin_id
-- ORDER BY price_date;
