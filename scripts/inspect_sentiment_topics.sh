#!/bin/bash

# ============================================
# Inspect YouTube sentiment Kafka topics
# ============================================

KAFKA_CONTAINER="kafka"

echo "============================================"
echo "Available YouTube sentiment Kafka topics"
echo "============================================"

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --list | grep "youtube"

echo ""
echo "============================================"
echo "Topic details"
echo "============================================"

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic youtube_raw_comments

echo ""

docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic youtube_sentiment_metrics
