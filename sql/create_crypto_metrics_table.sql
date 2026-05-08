DROP TABLE IF EXISTS crypto_metrics;

CREATE TABLE crypto_metrics (
    id SERIAL PRIMARY KEY,
    coin TEXT,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    avg_price DOUBLE PRECISION,
    min_price DOUBLE PRECISION,
    max_price DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
