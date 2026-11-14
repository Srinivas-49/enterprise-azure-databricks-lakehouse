import csv
import json
from pathlib import Path

import pytest

from integrations.kafka_orders import append_order_batch
from src.lakehouse import run, validate


def test_end_to_end_quality_deduplication_and_idempotent_replay(tmp_path):
    input_csv = tmp_path / "orders.csv"
    rows = [
        {"order_id": "1", "customer_id": "C", "order_date": "2026-11-14", "amount": "12.50", "status": "paid"},
        {"order_id": "1", "customer_id": "C", "order_date": "2026-11-14", "amount": "12.50", "status": "paid"},
        {"order_id": "2", "customer_id": "D", "order_date": "2026-11-14", "amount": "0.10", "status": "paid"},
        {"order_id": "3", "customer_id": "D", "order_date": "2026-11-14", "amount": "2.00", "status": "pending"},
        {"order_id": "4", "customer_id": "D", "order_date": "2026-11-14", "amount": "-1", "status": "paid"},
    ]
    append_order_batch(rows, input_csv)
    first = run(input_csv, tmp_path / "output")
    assert {k: first[k] for k in ("input_rows", "valid_unique_rows", "duplicate_rows", "quarantined_rows")} == {
        "input_rows": 5, "valid_unique_rows": 3, "duplicate_rows": 1, "quarantined_rows": 1}
    with Path(first["gold_path"]).open(newline="") as stream:
        assert list(csv.DictReader(stream)) == [{"order_date": "2026-11-14", "paid_orders": "2", "revenue_gbp": "12.60"}]
    assert run(input_csv, tmp_path / "output") == first
    assert json.loads((tmp_path / "output" / "run_metrics.json").read_text()) == first


def test_rejects_invalid_headers_and_nonfinite_amounts(tmp_path):
    with pytest.raises(ValueError):
        validate({"order_id": "1", "customer_id": "C", "order_date": "2026-11-14", "amount": "NaN", "status": "paid"})
    source = tmp_path / "bad.csv"
    source.write_text("order_id,amount\n1,5\n")
    with pytest.raises(ValueError, match="required columns"):
        run(source, tmp_path / "out")


def test_append_rejects_bad_payload(tmp_path):
    with pytest.raises(ValueError):
        append_order_batch(["bad"], tmp_path / "records.csv")
