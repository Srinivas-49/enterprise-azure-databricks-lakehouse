"""Static notebook guardrails, not cloud Spark verification."""
import ast
from pathlib import Path

NOTEBOOK = Path("databricks_free/05_real_nyc_311_inspections.py")

def test_python_syntax():
    ast.parse(NOTEBOOK.read_text(encoding="utf-8"))

def test_two_source_contract_and_aggregated_gold():
    source = NOTEBOOK.read_text(encoding="utf-8")
    for key in ("service_requests/page_*.jsonl", "restaurant_inspections/page_*.jsonl",
                "silver_311_quarantine", "silver_restaurant_quarantine",
                "silver_311_requests", "gold_borough_month_operations",
                "distinct_restaurant_inspections", "assert valid_n <= distinct_n"):
        assert key in source
