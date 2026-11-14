# Reproducible data-to-output evidence

Run the local synthetic demo without an Azure account:

    python -m scripts.generate_demo_data --out demo-data --orders 1000
    python -m scripts.validate_demo_pipeline --data demo-data --output demo-output
    python -m pytest -q

Inputs: orders.csv, customers.csv, payments.jsonl and manifest.json.
Current local ETL consumes orders.csv. Other feeds are explicit staged source fixtures, not falsely claimed as downstream joins.

Outputs: gold_daily_revenue.csv, quarantine.csv and run_metrics.json.
Expected default: 1000 unique accepted orders, 10 duplicates, 5 quarantined invalid rows, 1015 raw rows. Independent paid-revenue reconciliation must match the Gold output. Repeat a run and compare output metrics.

These are LOCAL CSV/SQLite tests. Databricks Free notebook demonstrates Delta history, MERGE, late events and Gold marts but requires a workspace run before claiming runtime success. Azure provisioning from infra/azure remains manual.
