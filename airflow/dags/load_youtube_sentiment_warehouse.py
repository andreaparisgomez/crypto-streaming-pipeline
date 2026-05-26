from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

SOURCE_DB_CONFIG = {
    "dbname": "crypto_db",
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": 5432,
}

WAREHOUSE_DB_CONFIG = {
    "dbname": os.getenv("WAREHOUSE_DB"),
    "user": os.getenv("WAREHOUSE_USER"),
    "password": os.getenv("WAREHOUSE_PASSWORD"),
    "host": os.getenv("WAREHOUSE_HOST"),
    "sslmode": os.getenv("WAREHOUSE_SSLMODE"),
}

default_args = {
    "owner": "andrea",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_dimensions():
    source_conn = psycopg2.connect(**SOURCE_DB_CONFIG)
    warehouse_conn = psycopg2.connect(**WAREHOUSE_DB_CONFIG)

    source_cursor = source_conn.cursor()
    warehouse_cursor = warehouse_conn.cursor()

    source_cursor.execute("""
        SELECT DISTINCT
            summary_date,
            source_query,
            channel_title,
            sentiment_label
        FROM daily_youtube_sentiment_summary;
    """)

    rows = source_cursor.fetchall()

    for summary_date, source_query, channel_title, sentiment_label in rows:
        warehouse_cursor.execute("""
            INSERT INTO dim_date (full_date, year, month, day, weekday)
            VALUES (%s, EXTRACT(YEAR FROM %s)::INT, EXTRACT(MONTH FROM %s)::INT, EXTRACT(DAY FROM %s)::INT, TO_CHAR(%s, 'Day'))
            ON CONFLICT (full_date) DO NOTHING;
        """, (summary_date, summary_date, summary_date, summary_date, summary_date))

        warehouse_cursor.execute("""
            INSERT INTO dim_source (source_query)
            VALUES (%s)
            ON CONFLICT (source_query) DO NOTHING;
        """, (source_query,))

        warehouse_cursor.execute("""
            INSERT INTO dim_channel (channel_title)
            VALUES (%s)
            ON CONFLICT (channel_title) DO NOTHING;
        """, (channel_title,))

        warehouse_cursor.execute("""
            INSERT INTO dim_sentiment_label (sentiment_label)
            VALUES (%s)
            ON CONFLICT (sentiment_label) DO NOTHING;
        """, (sentiment_label,))

    warehouse_conn.commit()

    source_cursor.close()
    warehouse_cursor.close()
    source_conn.close()
    warehouse_conn.close()

def load_fact_table():
    source_conn = psycopg2.connect(**SOURCE_DB_CONFIG)
    warehouse_conn = psycopg2.connect(**WAREHOUSE_DB_CONFIG)

    source_cursor = source_conn.cursor()
    warehouse_cursor = warehouse_conn.cursor()

    source_cursor.execute("""
        SELECT
            summary_date,
            source_query,
            channel_title,
            sentiment_label,
            comment_count,
            avg_sentiment_score,
            avg_weighted_sentiment_score,
            avg_engagement_score
        FROM daily_youtube_sentiment_summary;
    """)

    rows = source_cursor.fetchall()

    warehouse_cursor.execute("""
        DELETE FROM fact_youtube_sentiment_daily;
    """)

    for row in rows:
        (
            summary_date,
            source_query,
            channel_title,
            sentiment_label,
            comment_count,
            avg_sentiment_score,
            avg_weighted_sentiment_score,
            avg_engagement_score
        ) = row

        warehouse_cursor.execute("""
            INSERT INTO fact_youtube_sentiment_daily (
                date_id,
                source_id,
                channel_id,
                sentiment_id,
                comment_count,
                avg_sentiment_score,
                avg_weighted_sentiment_score,
                avg_engagement_score
            )
            SELECT
                d.date_id,
                s.source_id,
                c.channel_id,
                sl.sentiment_id,
                %s,
                %s,
                %s,
                %s
            FROM dim_date d
            JOIN dim_source s ON s.source_query = %s
            JOIN dim_channel c ON c.channel_title = %s
            JOIN dim_sentiment_label sl ON sl.sentiment_label = %s
            WHERE d.full_date = %s;
        """, (
            comment_count,
            avg_sentiment_score,
            avg_weighted_sentiment_score,
            avg_engagement_score,
            source_query,
            channel_title,
            sentiment_label,
            summary_date
        ))

    warehouse_conn.commit()

    source_cursor.close()
    warehouse_cursor.close()
    source_conn.close()
    warehouse_conn.close()

with DAG(
    dag_id="load_youtube_sentiment_warehouse",
    default_args=default_args,
    description="Load YouTube sentiment summary data into warehouse star schema",
    start_date=datetime(2026, 5, 25),
    schedule="@daily",
    catchup=False,
) as dag:

    load_dimensions_task = PythonOperator(
        task_id="load_dimensions",
        python_callable=load_dimensions,
    )

    load_fact_table_task = PythonOperator(
        task_id="load_fact_table",
        python_callable=load_fact_table,
    )

    load_dimensions_task >> load_fact_table_task
