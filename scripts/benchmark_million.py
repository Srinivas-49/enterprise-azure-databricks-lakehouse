"""Reproducible local synthetic CSV/SQLite lakehouse benchmark.

This tests the local Python implementation, NOT Databricks, Spark, Azure or production
hardware. Values vary across machines; report elapsed seconds and row counts.
"""
import argparse
import csv
import json
import platform
import tempfile
import time
from pathlib import Path
from src.lakehouse import run

def benchmark(unique=1000000, duplicates=100000, invalid=10000):
    if unique <= 0 or min(duplicates, invalid) < 0:
        raise ValueError("unique must be positive and other counts nonnegative")
    with tempfile.TemporaryDirectory(prefix="lakehouse_benchmark_") as folder:
        base = Path(folder)
        path = base / "orders.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["order_id", "customer_id", "order_date", "amount", "status"])
            for i in range(unique):
                writer.writerow([f"O{i}", f"C{i % 10000}", f"2026-11-{i % 28 + 1:02d}", f"{i % 200 + 0.5:.2f}", "paid"])
            for i in range(duplicates):
                writer.writerow([f"O{i % unique}", f"C{i % 10000}", f"2026-11-{i % 28 + 1:02d}", f"{i % 200 + 0.5:.2f}", "paid"])
            for i in range(invalid):
                writer.writerow([f"BAD{i}", f"C{i}", "2026-11-01", "-100", "paid"])
        started = time.perf_counter()
        metrics = run(path, base / "output")
        seconds = time.perf_counter() - started
        return {
            "benchmark_type": "synthetic_local_csv_sqlite_not_azure",
            "input_rows": unique + duplicates + invalid,
            "unique_valid_orders": metrics["valid_unique_rows"],
            "quarantined_rows": metrics["quarantined_rows"],
            "pipeline_seconds": round(seconds, 4),
            "rows_per_second": round((unique + duplicates + invalid) / seconds, 2),
            "python": platform.python_version(),
            "platform": platform.platform(),
        }

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--unique", type=int, default=1000000)
    p.add_argument("--duplicates", type=int, default=100000)
    p.add_argument("--invalid", type=int, default=10000)
    args = p.parse_args()
    print(json.dumps(benchmark(args.unique, args.duplicates, args.invalid), indent=2))
