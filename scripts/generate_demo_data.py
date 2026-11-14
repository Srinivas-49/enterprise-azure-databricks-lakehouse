"""Deterministic multi-source synthetic e-commerce fixture; no cloud services."""
import argparse
import csv
import json
from pathlib import Path
from random import Random

def generate(root, orders=1000, seed=42):
    if not 1 <= orders <= 100000:
        raise ValueError("orders must be from 1 to 100000")
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    rng = Random(seed)
    n_customers = min(100, orders)
    with (root / "customers.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "segment", "country"])
        for i in range(n_customers):
            writer.writerow([f"C{i:05d}", ("standard", "plus", "business")[i % 3], "GB"])
    records = []
    with (root / "payments.jsonl").open("w", encoding="utf-8") as f:
        for i in range(orders):
            amount = f"{rng.randrange(2, 300)}.{rng.randrange(100):02d}"
            status = "paid" if i % 5 else "pending"
            order_id = f"O{i:07d}"
            records.append([order_id, f"C{i % n_customers:05d}",
                            f"2026-11-{i % 28 + 1:02d}", amount, status])
            f.write(json.dumps({"payment_id": f"P{i:07d}", "order_id": order_id,
                                "amount": amount, "currency": "GBP",
                                "status": status}, sort_keys=True) + "\n")
    duplicates = min(10, orders)
    invalid = min(5, orders)
    records.extend(records[:duplicates])
    records.extend([[f"BAD{i:04d}", "C00000", "2026-11-01", "-1.00", "paid"]
                    for i in range(invalid)])
    with (root / "orders.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "amount", "status"])
        writer.writerows(records)
    metrics = {"synthetic_only": True, "seed": seed, "unique_orders": orders,
               "duplicate_orders": duplicates, "invalid_orders": invalid,
               "input_rows": len(records), "customers": n_customers, "payments": orders}
    (root / "manifest.json").write_text(json.dumps(metrics, sort_keys=True, indent=2))
    return metrics

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="demo-data")
    p.add_argument("--orders", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    print(json.dumps(generate(args.out, args.orders, args.seed), indent=2))
