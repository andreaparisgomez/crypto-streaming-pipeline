#!/bin/bash

docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto_prices \
  --partitions 1 \
  --replication-factor 1

docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto_metrics \
  --partitions 1 \
  --replication-factor 1
