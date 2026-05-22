import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "youtube_sentiment_metrics"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

cursor = conn.cursor()

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="youtube-postgres-consumer"
)

for message in consumer:
    event = message.value

    cursor.execute(
        """
        INSERT INTO youtube_sentiment_metrics (
            comment_id,
            platform,
            content_type,
            video_id,
            video_title,
            channel_title,
            author,
            comment_text,
            like_count,
            published_at,
            ingested_at,
            source_query,
            language,
            sentiment_score,
            sentiment_label,
            engagement_score,
            weighted_sentiment_score,
            processed_at
        )
        VALUES (
            %(comment_id)s,
            %(platform)s,
            %(content_type)s,
            %(video_id)s,
            %(video_title)s,
            %(channel_title)s,
            %(author)s,
            %(comment_text)s,
            %(like_count)s,
            %(published_at)s,
            %(ingested_at)s,
            %(source_query)s,
            %(language)s,
            %(sentiment_score)s,
            %(sentiment_label)s,
            %(engagement_score)s,
            %(weighted_sentiment_score)s,
            %(processed_at)s
        )
        ON CONFLICT (comment_id) DO NOTHING;
        """,
        event
    )

    conn.commit()
    print("Inserted sentiment event:", event["comment_id"])
