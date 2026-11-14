"""Test three-source joins, independent aggregation and negative quality gates."""
import json
import pytest
from scripts.generate_demo_data import generate
from src.multisource import run_multisource

def test_valid_end_to_end_join_and_payment_reconciliation(tmp_path):
    data = tmp_path / "data"
    generate(data, orders=70, seed=7)
    result = run_multisource(data, tmp_path / "output")
    assert result["passed"]
    assert result["input_payments"] == 70
    assert result["missing_or_unmatched_payment_orders"] == 0
    assert result["mismatched_payments"] == 0
    assert result["daily_revenue_gbp"] == result["segment_revenue_gbp"]
    assert len((tmp_path / "output" / "gold_customer_segments.csv").read_text().splitlines()) == 4
    assert run_multisource(data, tmp_path / "output") == result

def test_rejects_payment_amount_mismatch(tmp_path):
    data = tmp_path / "data"
    generate(data, orders=15)
    path = data / "payments.jsonl"
    lines = path.read_text().splitlines()
    event = json.loads(lines[0])
    event["amount"] = "9999.00"
    lines[0] = json.dumps(event)
    path.write_text("\n".join(lines) + "\n")
    with pytest.raises(AssertionError, match="Multi-source contract"):
        run_multisource(data, tmp_path / "out")

def test_rejects_orphan_customer(tmp_path):
    data = tmp_path / "data"
    generate(data, orders=12)
    path = data / "customers.csv"
    lines = path.read_text().splitlines()
    path.write_text("\n".join([lines[0], *lines[2:]]) + "\n")
    with pytest.raises(AssertionError, match="Multi-source contract"):
        run_multisource(data, tmp_path / "out")
