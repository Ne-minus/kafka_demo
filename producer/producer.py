import os
import time
import json
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")
INTERVAL = float(os.getenv("PRODUCE_INTERVAL_SEC", "1"))


def create_producer():
    last_err = None
    for attempt in range(1, 31):
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
            )
            return p
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[producer] Kafka not ready yet ({attempt}/30), retrying in 2s...")
            time.sleep(2)
    raise last_err


producer = create_producer()
print(f"[producer] connected to {BOOTSTRAP}, topic={TOPIC}")

i = 0
while True:
    payload = {
        "id": i,
        "ts": datetime.now(timezone.utc).isoformat(),
        "msg": "hello from producer",
    }
    producer.send(TOPIC, payload)
    producer.flush()
    print(f"[producer] sent: {payload}")
    i += 1
    time.sleep(INTERVAL)
