from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
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


def aggregate_youtube_sentiment():

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        TRUNCATE TABLE daily_youtube_sentiment_summary;
    """)

    cursor.execute("""
        INSERT INTO daily_youtube_sentiment_summary (
    summary_date,
    source_query,
    channel_title,
    sentiment_label,
    comment_count,
    avg_sentiment_score,
    avg_weighted_sentiment_score,
    avg_engagement_score
)
SELECT
    DATE(published_at) AS summary_date,
    source_query,
    channel_title,
    sentiment_label,
    COUNT(*) AS comment_count,
    AVG(sentiment_score),
    AVG(weighted_sentiment_score),
    AVG(engagement_score)
FROM youtube_sentiment_metrics
GROUP BY
    DATE(published_at),
    source_query,
    channel_title,
    sentiment_label;
""")

    conn.commit()

    cursor.close()
    conn.close()

with DAG(
    dag_id="daily_youtube_sentiment_summary",
    default_args=default_args,
    description="Aggregate daily YouTube sentiment metrics",
    start_date=datetime(2026, 5, 22),
    schedule="@daily",
    catchup=False,
) as dag:

    aggregate_task = PythonOperator(
        task_id="aggregate_youtube_sentiment",
        python_callable=aggregate_youtube_sentiment,
    )
