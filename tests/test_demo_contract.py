"""Test actual pipeline output, repeatability and negative quality gates."""
import json
import pytest
from scripts.generate_demo_data import generate
from scripts.validate_demo_pipeline import validate

def test_multi_source_data_to_gold(tmp_path):
    src = tmp_path / "sources"
    m = generate(src, orders=45, seed=13)
    assert m["input_rows"] == 60 and m["payments"] == 45
    result = validate(src, tmp_path / "output")
    assert result["passed"]
    assert result["counts"]["valid_unique_rows"] == 45
    assert result["counts"]["duplicate_rows"] == 10
    assert result["counts"]["quarantined_rows"] == 5
    assert validate(src, tmp_path / "output") == result

def test_deterministic_source_bytes(tmp_path):
    a, b = tmp_path / "a", tmp_path / "b"
    generate(a, orders=10, seed=19)
    generate(b, orders=10, seed=19)
    for name in ("orders.csv", "payments.jsonl", "customers.csv", "manifest.json"):
        assert (a / name).read_bytes() == (b / name).read_bytes()
    with pytest.raises(ValueError):
        generate(tmp_path / "oversized", orders=200000)

def test_quality_gate_detects_invalid_claims(tmp_path):
    src = tmp_path / "data"
    generate(src, orders=17)
    path = src / "manifest.json"
    m = json.loads(path.read_text())
    m["unique_orders"] = 1000
    path.write_text(json.dumps(m))
    with pytest.raises(AssertionError, match="Quality gates failed"):
        validate(src, tmp_path / "out")
