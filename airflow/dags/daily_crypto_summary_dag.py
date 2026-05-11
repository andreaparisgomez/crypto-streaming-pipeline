from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": 5432,
}


default_args = {
    "owner": "andrea",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_crypto_metrics():

    conn = psycopg2.connect(**DB_CONFIG)

    query = """
        SELECT
            coin,
            window_start,
            window_end,
            avg_price,
            min_price,
            max_price,
            volatility
        FROM crypto_metrics
        WHERE window_start >= CURRENT_DATE - INTERVAL '1 day'
          AND window_start < CURRENT_DATE;
    """

    df = pd.read_sql(query, conn)

    conn.close()

    df.to_csv("/tmp/crypto_metrics_extract.csv", index=False)


def transform_daily_summary():

    df = pd.read_csv("/tmp/crypto_metrics_extract.csv")

    if df.empty:
        raise ValueError("No data found.")

    df["window_start"] = pd.to_datetime(df["window_start"])

    df["summary_date"] = df["window_start"].dt.date

    summary = (
        df.groupby(["coin", "summary_date"])
        .agg(
            avg_price=("avg_price", "mean"),
            min_price=("min_price", "min"),
            max_price=("max_price", "max"),
            avg_volatility=("volatility", "mean"),
            number_of_windows=("coin", "count"),
        )
        .reset_index()
    )

    summary.to_csv("/tmp/daily_crypto_summary.csv", index=False)


def load_daily_summary():

    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    df = pd.read_csv("/tmp/daily_crypto_summary.csv")

    for _, row in df.iterrows():

        cur.execute(
            """
            INSERT INTO daily_crypto_summary (
                coin,
                summary_date,
                avg_price,
                min_price,
                max_price,
                avg_volatility,
                number_of_windows
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["coin"],
                row["summary_date"],
                row["avg_price"],
                row["min_price"],
                row["max_price"],
                row["avg_volatility"],
                int(row["number_of_windows"]),
            ),
        )

    conn.commit()

    cur.close()
    conn.close()


with DAG(
    dag_id="daily_crypto_summary",
    default_args=default_args,
    description="Daily ETL analytics pipeline for crypto metrics",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["crypto", "airflow", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_crypto_metrics",
        python_callable=extract_crypto_metrics,
    )

    transform_task = PythonOperator(
        task_id="transform_daily_summary",
        python_callable=transform_daily_summary,
    )

    load_task = PythonOperator(
        task_id="load_daily_summary",
        python_callable=load_daily_summary,
    )

    extract_task >> transform_task >> load_task
