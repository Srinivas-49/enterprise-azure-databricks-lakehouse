"""Reproducible local CSV/SQLite benchmark; NOT a Databricks or Azure benchmark."""
import argparse
import csv
import json
import tempfile
import time
from pathlib import Path
from src.lakehouse import run

def benchmark(unique=20000, duplicates=2000, invalid=400):
    with tempfile.TemporaryDirectory() as root:
        root = Path(root)
        path = root / "orders.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["order_id", "customer_id", "order_date", "amount", "status"])
            for i in range(unique):
                w.writerow([f"order-{i}", f"customer-{i%200}", f"2026-11-{(i%2)+1:02d}", f"{10+i%101:.2f}", "paid"])
            for i in range(duplicates):
                j = i % unique
                w.writerow([f"order-{j}", f"customer-{j%200}", f"2026-11-{(j%2)+1:02d}", f"{10+j%101:.2f}", "paid"])
            for i in range(invalid):
                w.writerow([f"invalid-{i}", "customer-0", "2026-11-01", "-1", "paid"])
        t0 = time.perf_counter()
        metrics = run(path, root / "output")
        elapsed = time.perf_counter() - t0
        return {"benchmark": "synthetic local CSV/SQLite, not Databricks",
                "metrics": metrics, "elapsed_seconds": round(elapsed, 3),
                "rows_per_second": round((unique + duplicates + invalid) / elapsed, 1)}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--unique", type=int, default=20000)
    ap.add_argument("--duplicates", type=int, default=2000)
    ap.add_argument("--invalid", type=int, default=400)
    args = ap.parse_args()
    if min(args.unique, args.duplicates, args.invalid) < 0 or args.unique == 0:
        ap.error("unique must be positive and counts nonnegative")
    print(json.dumps(benchmark(args.unique, args.duplicates, args.invalid), indent=2))
