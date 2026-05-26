#!/bin/bash

# ============================================
# Consume raw YouTube comments from Kafka
# ============================================

KAFKA_CONTAINER="kafka"
TOPIC="youtube_raw_comments"

echo "============================================"
echo "Consuming raw YouTube comments"
echo "============================================"

docker exec -it "$KAFKA_CONTAINER" kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic "$TOPIC" \
  --from-beginning
