import json
from src.lakehouse import run, validate

def test_validation():
    assert validate({"order_id": "x", "customer_id": "c", "order_date": "2026-01-01", "amount": "-1", "status": "paid"})[0] is None

def test_pipeline(tmp_path):
    m = run("data/orders.csv", tmp_path)
    assert m["input_rows"] == 5
    assert m["valid_unique_rows"] == 3
    assert m["quarantined_rows"] == 1
    assert json.loads((tmp_path / "gold.json").read_text())[0]["revenue"] == 125.5

def test_rerun_idempotent(tmp_path):
    run("data/orders.csv", tmp_path)
    first = (tmp_path / "gold.json").read_text()
    run("data/orders.csv", tmp_path)
    assert (tmp_path / "gold.json").read_text() == first
