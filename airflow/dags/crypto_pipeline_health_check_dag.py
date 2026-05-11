from datetime import datetime, timedelta
import os
import psycopg2
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator

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
    "retry_delay": timedelta(minutes=2),
}


def check_recent_crypto_metrics():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM crypto_metrics
        WHERE inserted_at >= NOW() - INTERVAL '15 minutes';
    """)

    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    if count == 0:
        raise ValueError("No recent rows found in crypto_metrics.")

    print(f"Recent crypto_metrics rows found: {count}")


def check_daily_summary_table_exists():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'daily_crypto_summary'
        );
    """)

    exists = cur.fetchone()[0]

    cur.close()
    conn.close()

    if not exists:
        raise ValueError("daily_crypto_summary table does not exist.")

    print("daily_crypto_summary table exists.")


def check_crypto_metrics_table_exists():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'crypto_metrics'
        );
    """)

    exists = cur.fetchone()[0]

    cur.close()
    conn.close()

    if not exists:
        raise ValueError("crypto_metrics table does not exist.")

    print("crypto_metrics table exists.")

def check_crypto_metrics_row_growth():

    import time
    import psycopg2
    from airflow.exceptions import AirflowException

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*)
        FROM crypto_metrics
    """)

    first_count = cursor.fetchone()[0]

    time.sleep(30)

    cursor.execute("""
        SELECT COUNT(*)
        FROM crypto_metrics
    """)

    second_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    if second_count <= first_count:
        raise AirflowException(
            f"No row growth detected. Count stayed at {first_count}"
        )

    print(
        f"Row growth detected: {first_count} → {second_count}"
    )

with DAG(
    dag_id="crypto_pipeline_health_check",
    description="Health checks for the crypto streaming pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["crypto", "monitoring", "health-check"],
) as dag:

    check_metrics_table = PythonOperator(
        task_id="check_crypto_metrics_table_exists",
        python_callable=check_crypto_metrics_table_exists,
    )

    check_summary_table = PythonOperator(
        task_id="check_daily_summary_table_exists",
        python_callable=check_daily_summary_table_exists,
    )

    check_recent_rows = PythonOperator(
        task_id="check_recent_crypto_metrics",
        python_callable=check_recent_crypto_metrics,
    )

    check_crypto_metrics_row_growth_task = PythonOperator(
        task_id="check_crypto_metrics_row_growth",
        python_callable=check_crypto_metrics_row_growth,
    )

    [check_metrics_table, check_summary_table] >> check_recent_rows >> check_crypto_metrics_row_growth_task
