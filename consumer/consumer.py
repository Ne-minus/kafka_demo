import os
import json
import time

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")
GROUP = os.getenv("KAFKA_GROUP", "demo-group")


def create_consumer():
    last_err = None
    for attempt in range(1, 31):
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            return c
        except NoBrokersAvailable as e:
            last_err = e
            print(f"[consumer] Kafka not ready yet ({attempt}/30), retrying in 2s...")
            time.sleep(2)
    raise last_err


consumer = create_consumer()
print(f"[consumer] connected to {BOOTSTRAP}, topic={TOPIC}, group={GROUP}")

for msg in consumer:
    print(
        f"[consumer] got: partition={msg.partition} offset={msg.offset} value={msg.value}"
    )
