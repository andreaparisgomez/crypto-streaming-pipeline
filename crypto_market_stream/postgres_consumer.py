import os
import json
from kafka import KafkaConsumer
import psycopg2
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
METRICS_TOPIC = "crypto_metrics"


POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


consumer = KafkaConsumer(
    METRICS_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="latest",
    enable_auto_commit=True
)


conn = psycopg2.connect(
    host=POSTGRES_HOST,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

cursor = conn.cursor()


for message in consumer:

    event = message.value

    cursor.execute("""
        INSERT INTO crypto_metrics (
            coin,
            window_start,
            window_end,
            avg_price,
            min_price,
            max_price,
            volatility
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        event.get("coin"),
        event.get("window_start"),
        event.get("window_end"),
        event.get("avg_price"),
        event.get("min_price"),
        event.get("max_price"),
        event.get("volatility")
    ))

    conn.commit()

    print("Inserted:", event)
