#!/bin/bash

# ============================================
# Create Kafka topics for YouTube sentiment stream
# ============================================

KAFKA_CONTAINER="kafka"
PARTITIONS=1
REPLICATION_FACTOR=1

RAW_TOPIC="youtube_raw_comments"
SENTIMENT_TOPIC="youtube_sentiment_metrics"

echo "Creating sentiment Kafka topics..."

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$RAW_TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$SENTIMENT_TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

echo "Sentiment Kafka topics created or already exist:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --list | grep "youtube"
