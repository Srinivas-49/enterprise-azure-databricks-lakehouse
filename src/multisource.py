"""Multi-source local integration with explicit financial and FK reconciliation.

This uses bounded synthetic fixture data and SQLite joins; it is NOT a
Databricks execution engine or a live ADF source. The existing src.lakehouse
pipeline remains the authoritative order validation/deduplication stage.
"""
from __future__ import annotations
import csv
import json
import sqlite3
from decimal import Decimal, InvalidOperation
from pathlib import Path
from src.lakehouse import run as run_orders, validate as validate_order

def amount_pennies(value):
    try:
        amount = Decimal(str(value))
        if not amount.is_finite() or amount <= 0 or amount.as_tuple().exponent < -2:
            raise ValueError("invalid amount")
        return int(amount * 100)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("invalid amount") from exc

def run_multisource(source_dir, output_dir):
    """Join validated orders to customers/payments, fail on any contract violation."""
    source_dir = Path(source_dir)
    output_dir = Path(output_dir)
    manifest = json.loads((source_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("synthetic_only") is not True:
        raise ValueError("Only synthetic manifests allowed")
    local = run_orders(source_dir / "orders.csv", output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE customers (customer_id TEXT PRIMARY KEY, segment TEXT NOT NULL)")
        db.execute("CREATE TABLE orders (order_id TEXT PRIMARY KEY, customer_id TEXT, amount_pennies INTEGER, status TEXT)")
        db.execute("CREATE TABLE payments (payment_id TEXT PRIMARY KEY, order_id TEXT UNIQUE)")

        with (source_dir / "customers.csv").open(newline="", encoding="utf-8") as f:
            for c in csv.DictReader(f):
                if not c.get("customer_id") or c.get("segment") not in {"standard", "plus", "business"}:
                    raise ValueError("invalid customer contract")
                try:
                    db.execute("INSERT INTO customers VALUES (?,?)",
                               (c["customer_id"], c["segment"]))
                except sqlite3.IntegrityError as exc:
                    raise ValueError("duplicate customer") from exc
        with (source_dir / "orders.csv").open(newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    key, _day, cents, status = validate_order(row)
                except (ValueError, TypeError):
                    continue  # already accounted for in order quarantine
                db.execute("INSERT OR IGNORE INTO orders VALUES (?,?,?,?)",
                           (key, row["customer_id"].strip(), cents, status))
        foreign_keys = db.execute("""
            SELECT COUNT(*) FROM orders o LEFT JOIN customers c
            ON o.customer_id=c.customer_id WHERE c.customer_id IS NULL
        """).fetchone()[0]

        mismatched, duplicate, malformed, total_payments = 0, 0, 0, 0
        with (source_dir / "payments.jsonl").open(encoding="utf-8") as f:
            for line in f:
                total_payments += 1
                try:
                    event = json.loads(line)
                    payment_id = event["payment_id"]
                    order_id = event["order_id"]
                    if not isinstance(payment_id, str) or not payment_id:
                        raise ValueError("invalid payment id")
                    source_amount = amount_pennies(event["amount"])
                    state = event["status"]
                except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                    malformed += 1
                    continue
                row = db.execute("SELECT amount_pennies, status FROM orders WHERE order_id=?", (order_id,)).fetchone()
                if row is None or row[0] != source_amount or row[1] != state:
                    mismatched += 1
                    continue
                try:
                    db.execute("INSERT INTO payments VALUES (?,?)", (payment_id, order_id))
                except sqlite3.IntegrityError:
                    duplicate += 1
        missing_payment = db.execute("""
            SELECT COUNT(*) FROM orders o LEFT JOIN payments p
            ON o.order_id=p.order_id WHERE p.order_id IS NULL
        """).fetchone()[0]
        rows = db.execute("""
            SELECT c.segment,
                   SUM(CASE WHEN o.status='paid' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN o.status='paid' THEN o.amount_pennies ELSE 0 END)
            FROM orders o JOIN customers c ON o.customer_id=c.customer_id
            GROUP BY c.segment ORDER BY c.segment
        """).fetchall()
    segment_file = output_dir / "gold_customer_segments.csv"
    with segment_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["segment", "paid_orders", "paid_revenue_gbp"])
        for segment, paid_count, cents in rows:
            writer.writerow([segment, paid_count or 0, f"{Decimal(cents or 0) / 100:.2f}"])
    with (output_dir / "gold_daily_revenue.csv").open(newline="", encoding="utf-8") as f:
        daily_amount = sum((Decimal(r["revenue_gbp"]) for r in csv.DictReader(f)), Decimal("0"))
    segment_amount = sum((Decimal(cents or 0) / 100 for _, _, cents in rows), Decimal("0"))
    checks = {
        "input_payments": total_payments,
        "valid_unique_orders": local["valid_unique_rows"],
        "missing_customer_references": foreign_keys,
        "missing_or_unmatched_payment_orders": missing_payment,
        "mismatched_payments": mismatched,
        "duplicate_payments": duplicate,
        "malformed_payments": malformed,
        "segment_revenue_gbp": str(segment_amount),
        "daily_revenue_gbp": str(daily_amount)
    }
    (output_dir / "payment_reconciliation.json").write_text(json.dumps(checks, indent=2), encoding="utf-8")
    if (foreign_keys or missing_payment or mismatched or duplicate or malformed
            or segment_amount != daily_amount
            or total_payments != manifest["payments"]):
        raise AssertionError("Multi-source contract reconciliation failed: " + json.dumps(checks))
    return {"passed": True, **checks}

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="demo-data")
    p.add_argument("--output", default="demo-output")
    a = p.parse_args()
    print(json.dumps(run_multisource(a.data, a.output), indent=2))
