# Three-source Gold integrity contract (local reference)

Pipeline flow:
1. Generate synthetic orders.csv, customers.csv, payments.jsonl and manifest.json from a fixed seed.
2. Run the existing CSV/SQLite order pipeline to validate and quarantine invalid orders, deduplicate on order_id, and calculate Gold daily revenue.
3. Enrich valid deduplicated orders with the customer dimension; reject unknown customer keys.
4. Reconcile each payment event to a unique valid order, requiring payment amount and status to equal the source order record.
5. Produce Gold customer segment revenue and a payment reconciliation document; assert all orders have exactly one matching payment and daily/segment revenue agrees to the penny.
6. Repeat to verify deterministic outputs; GitHub Actions archives the metrics and Gold results.

CLI (Python 3.11+):
    python -m scripts.generate_demo_data --out demo-data --orders 200
    python -m src.multisource --data demo-data --output demo-output
    python -m pytest -q

Output artifacts:
- gold_daily_revenue.csv and gold_customer_segments.csv
- quarantine.csv and run_metrics.json
- payment_reconciliation.json

This implementation is a bounded local SQLite **reference**. The payments file is a synthetic feed, not a regulated processor integration; no real money moves. It is not a live Databricks, Delta Lake, CDC, Azure Data Factory, or production performance demonstration. For large datasets, implement the joins and data contracts in Spark, use Delta tables and source CDC, and capture audited cloud run results before making live deployment claims.
