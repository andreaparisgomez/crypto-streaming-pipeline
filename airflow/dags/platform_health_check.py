from datetime import datetime, timedelta
import os
import psycopg2

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv


load_dotenv("/opt/airflow/.env")


OPERATIONAL_DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "crypto_db"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST", "host.docker.internal"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}


WAREHOUSE_DB_CONFIG = {
    "dbname": os.getenv("WAREHOUSE_DB"),
    "user": os.getenv("WAREHOUSE_USER"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
    "host": os.getenv("WAREHOUSE_HOST"),
    "port": 5432,
    "sslmode": os.getenv("WAREHOUSE_SSLMODE", "require"),
}


default_args = {
    "owner": "andrea",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_connection(config):
    return psycopg2.connect(**config)


def check_table_exists(config, table_name):
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                );
                """,
                (table_name,),
            )
            exists = cur.fetchone()[0]

    if not exists:
        raise ValueError(f"Table does not exist: {table_name}")

    print(f"Table exists: {table_name}")


def check_table_not_empty(config, table_name):
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cur.fetchone()[0]

    if row_count == 0:
        raise ValueError(f"Table is empty: {table_name}")

    print(f"{table_name} row count: {row_count}")


def check_recent_timestamp(config, table_name, timestamp_column, max_age_hours):
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX({timestamp_column}) FROM {table_name};")
            latest_timestamp = cur.fetchone()[0]

    if latest_timestamp is None:
        raise ValueError(f"No timestamp found in {table_name}.{timestamp_column}")

    age = datetime.utcnow() - latest_timestamp.replace(tzinfo=None)

    if age > timedelta(hours=max_age_hours):
        raise ValueError(
            f"Stale data in {table_name}. "
            f"Latest {timestamp_column}: {latest_timestamp}, age: {age}"
        )

    print(f"{table_name} latest {timestamp_column}: {latest_timestamp}")


def check_operational_crypto_pipeline():
    tables = ["crypto_metrics"]

    for table in tables:
        check_table_exists(OPERATIONAL_DB_CONFIG, table)
        check_table_not_empty(OPERATIONAL_DB_CONFIG, table)

    check_recent_timestamp(
        OPERATIONAL_DB_CONFIG,
        "crypto_metrics",
        "inserted_at",
        max_age_hours=24,
    )


def check_operational_sentiment_pipeline():
    check_table_exists(OPERATIONAL_DB_CONFIG, "youtube_sentiment_metrics")
    check_table_not_empty(OPERATIONAL_DB_CONFIG, "youtube_sentiment_metrics")

    check_recent_timestamp(
        OPERATIONAL_DB_CONFIG,
        "youtube_sentiment_metrics",
        "processed_at",
        max_age_hours=48,
    )


def check_warehouse_core_tables():
    warehouse_tables = [
        "dim_date",
        "dim_source",
        "dim_channel",
        "dim_sentiment_label",
        "fact_crypto_price_daily",
        "fact_youtube_sentiment_daily",
    ]

    for table in warehouse_tables:
        check_table_exists(WAREHOUSE_DB_CONFIG, table)
        check_table_not_empty(WAREHOUSE_DB_CONFIG, table)


def check_dashboard_views():
    dashboard_views = [
        "vw_crypto_price_daily",
        "vw_daily_sentiment_summary",
        "vw_youtube_sentiment_daily",
    ]

    for view in dashboard_views:
        check_table_exists(WAREHOUSE_DB_CONFIG, view)
        check_table_not_empty(WAREHOUSE_DB_CONFIG, view)


def check_crypto_data_quality():
    with get_connection(OPERATIONAL_DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM crypto_metrics
                WHERE coin IS NULL
                    OR window_start IS NULL
                    OR window_end IS NULL
                    OR avg_price IS NULL
                    OR min_price IS NULL
                    OR max_price IS NULL
                    OR inserted_at IS NULL
                    OR avg_price < 0
                    OR min_price < 0
                    OR max_price < 0
                    OR volatility < 0
                    OR window_end <= window_start;
            """
            )
            bad_rows = cur.fetchone()[0]

    if bad_rows > 0:
        raise ValueError(f"Crypto data quality check failed. Bad rows: {bad_rows}")

    print("Crypto data quality check passed.")


def check_sentiment_data_quality():
    with get_connection(OPERATIONAL_DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM youtube_sentiment_metrics
                WHERE comment_id IS NULL
                   OR platform IS NULL
                   OR content_type IS NULL
                   OR video_id IS NULL
                   OR comment_text IS NULL
                   OR published_at IS NULL
                   OR ingested_at IS NULL
                   OR sentiment_score IS NULL
                   OR sentiment_label IS NULL
                   OR engagement_score IS NULL
                   OR weighted_sentiment_score IS NULL
                   OR processed_at IS NULL
                   OR like_count < 0
                   OR sentiment_label NOT IN ('positive', 'neutral', 'negative');
                """
            )
            bad_rows = cur.fetchone()[0]

    if bad_rows > 0:
        raise ValueError(f"Sentiment data quality check failed. Bad rows: {bad_rows}")

    print("Sentiment data quality check passed.")


with DAG(
    dag_id="platform_health_check",
    default_args=default_args,
    description="Platform-level health checks for crypto, sentiment, warehouse and dashboard layers",
    start_date=datetime(2026, 5, 1),
    schedule="@hourly",
    catchup=False,
    tags=["monitoring", "healthcheck", "platform"],
) as dag:

    operational_crypto_check = PythonOperator(
        task_id="check_operational_crypto_pipeline",
        python_callable=check_operational_crypto_pipeline,
    )

    operational_sentiment_check = PythonOperator(
        task_id="check_operational_sentiment_pipeline",
        python_callable=check_operational_sentiment_pipeline,
    )

    crypto_quality_check = PythonOperator(
        task_id="check_crypto_data_quality",
        python_callable=check_crypto_data_quality,
    )

    sentiment_quality_check = PythonOperator(
        task_id="check_sentiment_data_quality",
        python_callable=check_sentiment_data_quality,
    )

    warehouse_check = PythonOperator(
        task_id="check_warehouse_core_tables",
        python_callable=check_warehouse_core_tables,
    )

    dashboard_check = PythonOperator(
        task_id="check_dashboard_views",
        python_callable=check_dashboard_views,
    )

operational_crypto_check >> crypto_quality_check
operational_sentiment_check >> sentiment_quality_check

[crypto_quality_check, sentiment_quality_check] >> warehouse_check
warehouse_check >> dashboard_check
