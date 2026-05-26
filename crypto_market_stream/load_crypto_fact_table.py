import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

SOURCE_DB_CONFIG = {
    "dbname": "crypto_db",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

WAREHOUSE_DB_CONFIG = {
    "dbname": os.getenv("WAREHOUSE_DB"),
    "user": os.getenv("WAREHOUSE_USER"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
    "host": os.getenv("WAREHOUSE_HOST"),
    "sslmode": os.getenv("WAREHOUSE_SSLMODE"),
}


def extract_crypto_metrics():
    query = """
        SELECT
            DATE(price_timestamp) AS full_date,
            coin_id,

            AVG(price_usd) AS avg_price,
            MIN(price_usd) AS min_price,
            MAX(price_usd) AS max_price,

            AVG(market_cap) AS avg_market_cap,
            AVG(total_volume) AS avg_volume

        FROM historical_crypto_prices
        GROUP BY DATE(price_timestamp), coin_id
        ORDER BY full_date, coin_id;
    """

    conn = psycopg2.connect(**SOURCE_DB_CONFIG)
    cur = conn.cursor()

    cur.execute(query)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


def get_date_mapping():
    query = """
        SELECT date_id, full_date
        FROM dim_date;
    """

    conn = psycopg2.connect(**WAREHOUSE_DB_CONFIG)
    cur = conn.cursor()

    cur.execute(query)

    mapping = {
        row[1]: row[0]
        for row in cur.fetchall()
    }

    cur.close()
    conn.close()

    return mapping


def load_fact_table(rows, date_mapping):
    insert_query = """
        INSERT INTO fact_crypto_price_daily (
            date_id,
            coin_id,
            avg_price,
            min_price,
            max_price,
            avg_market_cap,
            avg_volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)

        ON CONFLICT (date_id, coin_id)
        DO UPDATE SET
            avg_price = EXCLUDED.avg_price,
            min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price,
            avg_market_cap = EXCLUDED.avg_market_cap,
            avg_volume = EXCLUDED.avg_volume;
    """

    warehouse_rows = []

    for row in rows:
        full_date = row[0]
        date_id = date_mapping.get(full_date)

        if not date_id:
            continue

        warehouse_rows.append(
            (
                date_id,
                row[1],  # coin_id
                row[2],  # avg_price
                row[3],  # min_price
                row[4],  # max_price
                row[5],  # avg_market_cap
                row[6],  # avg_volume
            )
        )

    conn = psycopg2.connect(**WAREHOUSE_DB_CONFIG)
    cur = conn.cursor()

    cur.executemany(insert_query, warehouse_rows)

    conn.commit()

    cur.close()
    conn.close()

    print(f"Loaded {len(warehouse_rows)} rows into fact table")


def main():
    rows = extract_crypto_metrics()
    print(f"Extracted {len(rows)} aggregated rows")

    date_mapping = get_date_mapping()

    load_fact_table(rows, date_mapping)


if __name__ == "__main__":
    main()
