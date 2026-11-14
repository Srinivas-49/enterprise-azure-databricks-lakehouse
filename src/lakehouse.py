"""Dependency-free, bounded-memory CSV/SQLite reference lakehouse.

This is a local development implementation, not a Databricks/Delta runtime.
Each invocation reconstructs the output from source data, so reruns are repeatable.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
import tempfile
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

FIELDS = ("order_id", "customer_id", "order_date", "amount", "status")
STATUS = {"paid", "pending", "refunded"}


def validate(row: dict[str, str]) -> tuple[str, str, int, str]:
    """Return validated fields, storing currency as integer pennies."""
    if any(not (row.get(field) or "").strip() for field in FIELDS):
        raise ValueError("missing required field")
    order_id = row["order_id"].strip()
    order_date = date.fromisoformat(row["order_date"].strip()).isoformat()
    status = row["status"].strip().lower()
    if status not in STATUS:
        raise ValueError("unsupported status")
    try:
        value = Decimal(row["amount"].strip())
        if not value.is_finite() or value <= 0 or value.as_tuple().exponent < -2:
            raise ValueError("invalid amount")
        pennies = int(value * 100)
        if pennies > 100_000_000_000_00:
            raise ValueError("amount too large")
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid amount") from exc
    return order_id, order_date, pennies, status


def run(input_csv: str | Path, output_dir: str | Path) -> dict:
    """Build a deduplicated Silver table and daily paid-order Gold aggregate.

    Invalid records go to quarantine; duplicate order IDs keep the first valid row.
    Output files replace prior runs. No Azure credentials or cloud charges required.
    """
    source = Path(input_csv)
    destination = Path(output_dir)
    if not source.is_file():
        raise FileNotFoundError(source)
    destination.mkdir(parents=True, exist_ok=True)
    gold_file = destination / "gold_daily_revenue.csv"
    quarantine_file = destination / "quarantine.csv"
    counters = {"input_rows": 0, "valid_unique_rows": 0, "duplicate_rows": 0, "quarantined_rows": 0}
    with tempfile.TemporaryDirectory(prefix="lakehouse_") as temp:
        with sqlite3.connect(str(Path(temp) / "working.db")) as db:
            db.execute("CREATE TABLE silver (order_id TEXT PRIMARY KEY, order_date TEXT NOT NULL, amount_pennies INTEGER NOT NULL, status TEXT NOT NULL)")
            with source.open(newline="", encoding="utf-8-sig") as stream, (Path(temp) / "quarantine.csv").open("w", newline="", encoding="utf-8") as rejected:
                reader = csv.DictReader(stream)
                if not reader.fieldnames or not set(FIELDS).issubset(reader.fieldnames):
                    raise ValueError("CSV missing required columns")
                writer = csv.DictWriter(rejected, fieldnames=[*FIELDS, "reason"], extrasaction="ignore")
                writer.writeheader()
                for row in reader:
                    counters["input_rows"] += 1
                    try:
                        order_id, order_date, pennies, status = validate(row)
                    except (ValueError, TypeError) as exc:
                        counters["quarantined_rows"] += 1
                        writer.writerow({**row, "reason": str(exc)})
                        continue
                    result = db.execute("INSERT OR IGNORE INTO silver VALUES (?, ?, ?, ?)", (order_id, order_date, pennies, status))
                    if result.rowcount:
                        counters["valid_unique_rows"] += 1
                    else:
                        counters["duplicate_rows"] += 1
            daily = db.execute("SELECT order_date, COUNT(*), SUM(amount_pennies) FROM silver WHERE status = 'paid' GROUP BY order_date ORDER BY order_date")
            with (Path(temp) / "gold.csv").open("w", newline="", encoding="utf-8") as output:
                writer = csv.writer(output)
                writer.writerow(["order_date", "paid_orders", "revenue_gbp"])
                for day, count, pennies in daily:
                    writer.writerow([day, count, f"{Decimal(pennies) / 100:.2f}"])
        os.replace(Path(temp) / "gold.csv", gold_file)
        os.replace(Path(temp) / "quarantine.csv", quarantine_file)
    counters.update({"gold_path": str(gold_file), "quarantine_path": str(quarantine_file)})
    (destination / "run_metrics.json").write_text(json.dumps(counters, indent=2), encoding="utf-8")
    return counters
