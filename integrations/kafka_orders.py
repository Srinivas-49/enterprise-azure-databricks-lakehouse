"""Optional Apache Kafka landing adapter for the local medallion pipeline.

The pure CSV append function can be tested without a broker. The live adapter
requires kafka-python and Kafka credentials; it has not been cloud-deployed.
"""
import csv
import json
import os
from pathlib import Path

COLUMNS = ["order_id", "customer_id", "order_date", "amount", "status"]

def append_order_batch(events, destination):
    """Append a durable CSV batch; downstream deduplicates by order_id."""
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    exists = target.exists() and target.stat().st_size > 0
    with target.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for event in events:
            if not isinstance(event, dict):
                raise ValueError("Kafka order event must be a JSON object")
            writer.writerow({key: event.get(key, "") for key in COLUMNS})
        handle.flush()
        os.fsync(handle.fileno())
    return target

def consume_kafka_to_csv(destination, topic, bootstrap_servers, max_messages=100):
    """Persist a bounded batch before offset commit; at-least-once replay safe."""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers.split(","),
                             enable_auto_commit=False, auto_offset_reset="earliest",
                             consumer_timeout_ms=5000,
                             value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    batch = []
    try:
        for record in consumer:
            batch.append(record.value)
            if len(batch) >= max_messages:
                break
        if batch:
            append_order_batch(batch, destination)
            consumer.commit()
        return len(batch)
    finally:
        consumer.close()

if __name__ == "__main__":
    topic = os.environ["KAFKA_ORDERS_TOPIC"]
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    count = consume_kafka_to_csv(os.environ.get("BRONZE_ORDERS_CSV", "data/kafka_orders.csv"),
                                 topic, bootstrap)
    print(f"Landed {count} Kafka events")
