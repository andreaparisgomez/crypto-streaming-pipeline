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


def extract_dates():
    query = """
        SELECT DISTINCT
            DATE(price_timestamp) AS full_date,
            EXTRACT(YEAR FROM price_timestamp)::INTEGER AS year,
            EXTRACT(MONTH FROM price_timestamp)::INTEGER AS month,
            EXTRACT(DAY FROM price_timestamp)::INTEGER AS day,
            TRIM(TO_CHAR(price_timestamp, 'Day')) AS weekday
        FROM historical_crypto_prices
        ORDER BY full_date;
    """

    conn = psycopg2.connect(**SOURCE_DB_CONFIG)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows


def load_dates(rows):
    query = """
        INSERT INTO dim_date (
            full_date,
            year,
            month,
            day,
            weekday
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (full_date) DO NOTHING;
    """

    conn = psycopg2.connect(**WAREHOUSE_DB_CONFIG)
    cur = conn.cursor()
    cur.executemany(query, rows)
    conn.commit()
    cur.close()
    conn.close()


def main():
    rows = extract_dates()
    print(f"Extracted {len(rows)} dates from crypto_db")

    load_dates(rows)
    print("Loaded dates into crypto_warehouse.dim_date")


if __name__ == "__main__":
    main()
