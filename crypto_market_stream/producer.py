# imports
import requests
import pandas as pd
from datetime import datetime
import time
from kafka import KafkaProducer
import json

# configuration

URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=bitcoin,ethereum,solana"

KAFKA_TOPIC = "crypto_prices"

FETCH_INTERVAL = 60

# Kafka producer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# functions

def fetch_crypto_prices():

    response = requests.get(URL)

    if response.status_code != 200:
        print(f"API request failed: {response.status_code}")
        return None

    json_file = response.json()

    rows = []

    for item in json_file:

        row = {
            'coin': item.get('id'),
            'price_usd': item.get('current_price'),
            'market_cap': item.get('market_cap'),
            'volume': item.get('total_volume'),
            'change_24h': item.get('price_change_percentage_24h'),
            'timestamp': str(datetime.now()),
        }

        producer.send(KAFKA_TOPIC, value=row)

        rows.append(row)

    producer.flush()

    df = pd.DataFrame(rows)

    return df

# main loop

while True:

    df = fetch_crypto_prices()

    if df is not None and not df.empty:
        print("Sent batch at:", datetime.now())
        print(df)

    time.sleep(FETCH_INTERVAL)
