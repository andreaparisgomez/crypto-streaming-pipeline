"""
Legacy Python-only streaming processor.

This version manually calculated rolling metrics using
Kafka consumers/producers and in-memory state management
before migrating to a PySpark Structured Streaming architecture.
"""

# imports
import json
import statistics
from kafka import KafkaConsumer, KafkaProducer
import psycopg2

# state storage (stores last 5 prices per coin)
from collections import defaultdict, deque

price_history = defaultdict(lambda: deque(maxlen=5))


# definitions 
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# code
conn = psycopg2.connect(
    host="localhost",
    database="crypto_db",
    user="postgres",
    password="XXXXXXXX"       # insert your password here
)

cursor = conn.cursor()

for message in consumer:
    event = message.value
    
    coin = event['coin']
    price = event['price_usd']
    
    # store history
    price_history[coin].append(price)
    
    prices = list(price_history[coin])

    if len(prices) >= 2:
        pct_change = (prices[-1] - prices[-2]) / prices[-2]
    else:
        pct_change = None

    rolling_avg = sum(prices) / len(prices)

    if len(prices) >= 3:
        volatility = statistics.stdev(prices)
    else:
        volatility = None

    processed_event = {
        "coin": coin,
        "price": price,
        "pct_change": pct_change,
        "rolling_avg": rolling_avg,
        "volatility": volatility
    }
    producer.send('crypto_metrics', value=processed_event)

    print(processed_event)

    cursor.execute("""
        INSERT INTO crypto_metrics (coin, price, pct_change, rolling_avg, volatility)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        processed_event["coin"],
        processed_event["price"],
        processed_event["pct_change"],
        processed_event["rolling_avg"],
        processed_event["volatility"]
    ))

    conn.commit()
    
    print("Inserted:", processed_event)

    producer.flush()
