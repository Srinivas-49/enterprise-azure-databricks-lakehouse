"""Landing adapter shared by REST and Kafka clients.

This is append-only CSV landing, not a Kafka consumer or exactly-once sink.
Consumers must manage offsets and replay; src.lakehouse.run deduplicates order IDs.
"""
import csv
from pathlib import Path

FIELDS = ("order_id", "customer_id", "order_date", "amount", "status")


def append_order_batch(orders, output_csv):
    output = Path(output_csv)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not isinstance(orders, list) or any(not isinstance(order, dict) for order in orders):
        raise ValueError("orders must be a list of objects")
    with output.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        if handle.tell() == 0:
            writer.writeheader()
        for order in orders:
            writer.writerow({field: order.get(field, "") for field in FIELDS})
    return len(orders)
