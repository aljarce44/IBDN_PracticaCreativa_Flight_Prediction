import sys
import json
from kafka import KafkaProducer

if len(sys.argv) < 3:
  print("Uso: python send_kafka_message.py <topic> <json_file>")
  sys.exit(1)

topic = sys.argv[1]
json_file = sys.argv[2]

with open(json_file, "r", encoding="utf-8") as f:
  message = json.load(f)

producer = KafkaProducer(
  bootstrap_servers=["127.0.0.1:9092"],
  value_serializer=lambda v: json.dumps(v).encode("utf-8"),
  api_version=(3, 9, 0),
  request_timeout_ms=30000,
  max_block_ms=30000,
  retries=3
)

future = producer.send(topic, value=message)
future.get(timeout=30)
producer.flush()
producer.close()

print("OK")