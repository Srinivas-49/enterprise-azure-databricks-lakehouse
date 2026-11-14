"""Deterministic local medallion pipeline; Azure adapter is a separate example."""
import csv
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

REQUIRED = {"order_id", "customer_id", "order_date", "amount", "status"}

def ingest_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not REQUIRED.issubset(reader.fieldnames or []):
            raise ValueError("missing columns: " + ", ".join(sorted(REQUIRED - set(reader.fieldnames or []))))
        return list(reader)

def validate(row):
    try:
        amount = float(row["amount"])
        datetime.strptime(row["order_date"], "%Y-%m-%d")
        if not row["order_id"] or not row["customer_id"] or amount < 0:
            return None, "invalid identifier or amount"
        if row["status"] not in {"paid", "pending", "refunded"}:
            return None, "invalid status"
        return {**row, "amount": round(amount, 2)}, None
    except (ValueError, TypeError, KeyError):
        return None, "invalid data type or date"

def run(input_csv, output_dir):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    bronze = ingest_csv(input_csv)
    silver_by_id = {}
    rejected = []
    for row in bronze:
        clean, reason = validate(row)
        if reason:
            rejected.append({"row": row, "reason": reason})
        else:
            silver_by_id[clean["order_id"]] = clean
    silver = list(silver_by_id.values())
    gold = {}
    for row in silver:
        if row["status"] != "paid":
            continue
        agg = gold.setdefault(row["order_date"], {"order_date": row["order_date"], "orders": 0, "revenue": 0.0})
        agg["orders"] += 1
        agg["revenue"] += row["amount"]
    for agg in gold.values():
        agg["revenue"] = round(agg["revenue"], 2)
    for name, rows in (("bronze", bronze), ("silver", silver), ("gold", list(gold.values())), ("quarantine", rejected)):
        (out / (name + ".json")).write_text(json.dumps(rows, indent=2))
    with sqlite3.connect(out / "analytics.db") as db:
        db.execute("CREATE TABLE IF NOT EXISTS daily_sales (order_date TEXT PRIMARY KEY, orders INTEGER, revenue REAL)")
        db.execute("DELETE FROM daily_sales") # ensure removed dates disappear on rerun
        for g in gold.values():
            db.execute("INSERT INTO daily_sales VALUES (?,?,?)", (g["order_date"], g["orders"], g["revenue"]))
    metrics = {
        "input_rows": len(bronze), "valid_unique_rows": len(silver),
        "quarantined_rows": len(rejected), "gold_days": len(gold),
        "run_timestamp_utc": datetime.now(timezone.utc).isoformat()
    }
    (out / "metrics.json").write_text(json.dumps(metrics, indent=2))
    return metrics
