#!/bin/bash

# ============================================
# Consume processed YouTube sentiment metrics
# ============================================

KAFKA_CONTAINER="kafka"
TOPIC="youtube_sentiment_metrics"

echo "============================================"
echo "Consuming processed YouTube sentiment metrics"
echo "============================================"

docker exec -it "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning
