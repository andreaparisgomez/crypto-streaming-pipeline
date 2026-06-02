"""
One-time historical data backfill from CoinGecko
into historical_crypto_prices.
"""

import os
import time
from datetime import datetime, timezone

import requests
import psycopg2
from dotenv import load_dotenv


load_dotenv()

DB_CONFIG = {
    "dbname": "crypto_db",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

COINS = ["bitcoin", "ethereum", "solana"]
CURRENCY = "usd"
DAYS = 365

BASE_URL = "https://api.coingecko.com/api/v3"


def fetch_market_chart(coin_id):
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"

    params = {
        "vs_currency": CURRENCY,
        "days": DAYS,
        "interval": "daily",
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def transform_market_chart(coin_id, data):
    prices = data.get("prices", [])
    market_caps = data.get("market_caps", [])
    volumes = data.get("total_volumes", [])

    rows = []

    for i in range(len(prices)):
        timestamp_ms = prices[i][0]

        price_timestamp = datetime.fromtimestamp(
            timestamp_ms / 1000,
            tz=timezone.utc
        )

        rows.append(
            (
                coin_id,
                CURRENCY,
                price_timestamp,
                prices[i][1],
                market_caps[i][1] if i < len(market_caps) else None,
                volumes[i][1] if i < len(volumes) else None,
            )
        )

    return rows


def insert_rows(rows):
    insert_query = """
        INSERT INTO historical_crypto_prices (
            coin_id,
            currency,
            price_timestamp,
            price_usd,
            market_cap,
            total_volume
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (coin_id, currency, price_timestamp)
        DO UPDATE SET
            price_usd = EXCLUDED.price_usd,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            ingested_at = CURRENT_TIMESTAMP;
    """

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.executemany(insert_query, rows)

    conn.commit()
    cur.close()
    conn.close()


def main():
    for coin_id in COINS:
        print(f"Fetching historical data for {coin_id}...")

        data = fetch_market_chart(coin_id)
        rows = transform_market_chart(coin_id, data)

        insert_rows(rows)

        print(f"Inserted/updated {len(rows)} rows for {coin_id}")

        time.sleep(2)

    print("Historical crypto backfill complete.")


if __name__ == "__main__":
    main()
