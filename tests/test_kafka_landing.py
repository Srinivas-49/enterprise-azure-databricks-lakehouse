import csv

from integrations.kafka_orders import append_order_batch
from src.lakehouse import run

def test_kafka_adapter_to_lakehouse(tmp_path):
    source = tmp_path / "orders.csv"
    events = [
        {"order_id": "A1", "customer_id": "C1", "order_date": "2026-11-14", "amount": 12, "status": "paid"},
        {"order_id": "A1", "customer_id": "C1", "order_date": "2026-11-14", "amount": 12, "status": "paid"},
        {"order_id": "A2", "customer_id": "C2", "order_date": "2026-11-14", "amount": -1, "status": "paid"},
    ]
    append_order_batch(events, source)
    append_order_batch(events[:1], source)
    with source.open() as f:
        assert len(list(csv.DictReader(f))) == 4
    result = run(source, tmp_path / "out")
    assert result["input_rows"] == 4
    assert result["valid_unique_rows"] == 1
    assert result["quarantined_rows"] == 1
