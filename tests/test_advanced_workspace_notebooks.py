"""Offline syntax and feature-contract checks; live Spark must be verified separately."""
import ast
from pathlib import Path

NOTEBOOK = Path("databricks_free/03_multisource_spark_gold.py")

def test_multisource_notebook_compiles():
    ast.parse(NOTEBOOK.read_text(encoding="utf-8"))

def test_multisource_quality_and_gold_stages():
    source = NOTEBOOK.read_text(encoding="utf-8")
    for required in ("bronze_orders", "bronze_customers", "bronze_payments",
                     "silver_quarantine", "silver_reconciled_orders",
                     "gold_customer_segments", "gold_daily_revenue",
                     "Gold financial reconciliation failed", "assert counts =="):
        assert required in source
