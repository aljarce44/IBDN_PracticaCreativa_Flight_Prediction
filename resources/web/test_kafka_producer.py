from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=["127.0.0.1:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    api_version=(3, 9, 0),
    request_timeout_ms=30000,
    max_block_ms=30000
)

msg = {
    "id": "test-python-001",
    "DepDelay": 10,
    "Carrier": "AA",
    "Origin": "ATL",
    "Dest": "SFO"
}

producer.send("flight-delay-ml-request", msg)
producer.flush()

print("Mensaje enviado correctamente a Kafka")
time.sleep(1)
producer.close()