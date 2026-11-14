"""Offline preflight only: validates notebook syntax and required demonstration stages.

These are NOT Databricks execution or Delta integration tests.
"""
import ast
from pathlib import Path

SOURCE = Path("databricks_free/01_end_to_end_lakehouse.py")

def test_notebook_is_valid_python():
    ast.parse(SOURCE.read_text(encoding="utf-8"))

def test_stages_present():
    source = SOURCE.read_text(encoding="utf-8")
    for section in ("bronze_order_events", "silver_quarantine", "silver_order_event_history",
                    "silver_orders", "gold_daily_revenue", "gold_customer_360",
                    "gold_order_status", "ops_pipeline_audit"):
        assert section in source

def test_replay_and_ordering_invariants_present():
    source = SOURCE.read_text(encoding="utf-8")
    assert "process_batch(\"seed\", seed_batch)" in source
    assert source.count("process_batch(\"change\", change_batch)") == 2
    assert "s.updated_at > t.updated_at" in source
    assert "WHEN NOT MATCHED THEN INSERT *" in source
    assert "silver_order_event_history" in source
