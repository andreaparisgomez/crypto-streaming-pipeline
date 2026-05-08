#!/bin/bash

docker exec -it broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic crypto_metrics \
  --from-beginning
