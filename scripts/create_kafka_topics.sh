#!/bin/bash

docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic crypto_prices \
  --partitions 1 \
  --replication-factor 1

docker exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic crypto_metrics \
  --partitions 1 \
  --replication-factor 1
