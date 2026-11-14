"""Check actual data-to-Gold output, counts and financial reconciliation."""
import argparse
import csv
import json
from decimal import Decimal
from pathlib import Path
from src.lakehouse import run

def validate(source_dir, result_dir):
    source_dir = Path(source_dir)
    result_dir = Path(result_dir)
    manifest = json.loads((source_dir / "manifest.json").read_text())
    if manifest.get("synthetic_only") is not True:
        raise ValueError("Synthetic-only demo; refusing unclassified sources")
    output = run(source_dir / "orders.csv", result_dir)
    expected = {"input_rows": manifest["input_rows"],
                "valid_unique_rows": manifest["unique_orders"],
                "duplicate_rows": manifest["duplicate_orders"],
                "quarantined_rows": manifest["invalid_orders"]}
    errors = {field: (output[field], expected[field]) for field in expected
              if output[field] != expected[field]}
    with (source_dir / "orders.csv").open(newline="", encoding="utf-8") as f:
        orders = list(csv.DictReader(f))
    unique = {}
    for row in orders:
        if not row["order_id"].startswith("BAD"):
            unique.setdefault(row["order_id"], row)
    expected_revenue = sum((Decimal(o["amount"]) for o in unique.values()
                            if o["status"] == "paid"), Decimal("0"))
    with (result_dir / "gold_daily_revenue.csv").open(newline="", encoding="utf-8") as f:
        actual_revenue = sum((Decimal(row["revenue_gbp"]) for row in csv.DictReader(f)),
                             Decimal("0"))
    if actual_revenue != expected_revenue:
        errors["revenue"] = (str(actual_revenue), str(expected_revenue))
    if errors:
        raise AssertionError("Quality gates failed: " + json.dumps(errors))
    return {"passed": True, "counts": expected, "gold_revenue_gbp": str(actual_revenue)}

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="demo-data")
    p.add_argument("--output", default="demo-output")
    a = p.parse_args()
    print(json.dumps(validate(a.data, a.output), indent=2))
