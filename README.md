# Enterprise Azure Databricks Lakehouse — reproducible reference pipeline

> **2026 portfolio upgrade:** Multi-source synthetic input generation, local Gold reconciliation, automated evidence artifacts, and Azure Bicep for a *single shared* ADLS Gen2 / Data Factory / Key Vault learning environment. Databricks workspace provisioning is optional and disabled by default. **No Azure resources are claimed deployed.**


### Three-source Gold reconciliation (local reference)

The synthetic customer and payment feeds are now joined with deduplicated orders in
[src/multisource.py](src/multisource.py). In addition to daily revenue, the
pipeline generates Gold customer-segment revenue and a payment reconciliation report.
It rejects missing customer keys, unmatched payments, payment amount/status mismatches
and financial totals that do not tie out. Unit tests include tampered payments and orphan
customer records.

    python -m scripts.generate_demo_data --out demo-data --orders 200
    python -m src.multisource --data demo-data --output demo-output
    python -m pytest -q

See [the exact three-source data contract](docs/THREE_SOURCE_GOLD_CONTRACT.md).
This is verified locally with SQLite, not a claim that multi-source joins have
been deployed on Azure Databricks.

### Verified local project demonstration

    python -m scripts.generate_demo_data --out demo-data --orders 200
    python -m scripts.validate_demo_pipeline --data demo-data --output demo-output
    python -m pytest -q

Inputs: synthetic orders, customers, payment events and a versioned manifest. The local reference **processes orders** and stages the other two sources; it does not yet join them. Outputs: Gold paid revenue, quarantined rows and run metrics. GitHub CI uploads these outputs as a reviewable artifact.

- [Azure foundation and $150-credit runbook](infra/azure/README.md)
- [Exact validation contract](docs/PORTFOLIO_VALIDATION.md)
- [Databricks Free Edition notebook](databricks_free/01_end_to_end_lakehouse.py)



**Project status:** A locally runnable Python/SQLite ETL reference and Azure/Databricks architecture examples. Databricks, Airflow, Kafka and Terraform production deployments have not been validated.

## What is implemented

The tested local ingestion path supports CSV and an HTTPS REST landing adapter, validates order schemas and values, separates invalid records into quarantine, deduplicates order IDs in a SQLite-backed Silver layer, and writes daily paid-order Gold revenue aggregates. Monetary values are validated with Decimal and aggregated as integer pennies to avoid float rounding in the local implementation.

For REST ingestion, see integrations/rest_orders.py. Append-only landing in integrations/kafka_orders.py is a reusable adapter, **not** a working Kafka consumer or an exactly-once guarantee. Cloud architecture and optimization options are documented under docs/.

## Run locally

1. Install Python 3.11+ and pytest.
2. Run: python -m pytest -q
3. Run a small synthetic benchmark: python -m scripts.benchmark_million --unique 1000 --duplicates 100 --invalid 10
4. For the original 1.11-million-row local benchmark: python -m scripts.benchmark_million --unique 1000000 --duplicates 100000 --invalid 10000

The benchmark measures local CSV/SQLite throughput, **not Spark, Azure Databricks, production latency or a cloud SLA**. No cloud credentials are needed for local tests.

## Engineering contract

- Required fields: order_id, customer_id, order_date (ISO date), amount (positive, max two decimal places) and status (paid, pending, refunded).
- Duplicate primary keys preserve the first valid row. Replays rebuild from source and generate deterministic results.
- Quarantined invalid records retain their source fields and a rejection reason. Treat these files as sensitive when using actual data.
- Gold revenue sums **paid** orders only. Output is gold_daily_revenue.csv plus quarantine.csv and run_metrics.json.
- A real Kafka deployment requires authenticated consumption, persisted offsets, replay guarantees and dead-letter handling; production cloud storage requires access controls, encryption and lineage.

## Architecture

Source CSV or HTTPS API → landing → schema checks / quarantine → SQLite-backed Silver deduplication → Gold daily aggregates.

See docs/ADR-001-LAKEHOUSE-ARCHITECTURE.md for the intended Azure Databricks medallion design and docs/LOCAL_BENCHMARK_1_11M.md for historical local measurements. Infrastructure files remain examples and are not proof of a live deployment.

## Operational next steps

Validate Delta Lake append and MERGE semantics on an actual Databricks workspace; configure Unity Catalog permissions and managed identity; add source-specific checkpointing and retries; collect measured Spark profiles and compute bills before making performance or cost claims.
