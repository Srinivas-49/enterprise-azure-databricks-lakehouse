# Enterprise Azure Databricks Lakehouse

**Status:** Executable, locally tested reference pipeline with an optional, **not live-tested**, Azure Databricks adapter. All sample data is synthetic.

## Problem and solution
Dirty, duplicated transactional CSVs generate unreliable business KPIs. The reference implementation validates and quarantines invalid rows, deduplicates by order ID, writes Bronze/Silver/Gold artifacts, and serves daily revenue aggregates through SQLite.

**Architecture:** CSV -> Bronze -> validation/quarantine -> Silver (deduplication) -> Gold (daily sales KPIs) -> SQLite.

## Run
```bash
python -m scripts.run_demo
python -m pytest -q
```
No paid services or cloud credentials needed. Generated outputs in `outputs/` are intentionally gitignored.

## Reliability
Schema checks, deterministic last-write-wins, invalid-row quarantine, repeatable output replacement, and automated unit tests. The Azure notebook in `azure/databricks_medallion.py` is an integration starting point; **not evidence of a deployed lakehouse**.

## Production-extension roadmap
ADF orchestration, ADLS Gen2 access, Delta MERGE and change data capture, secrets management, event-time conflict resolution, data quality SLAs, observability, access control and incremental loads.

## Interview topics
Medallion layers, Delta transactions, ACID, schema evolution, partition sizing, duplicate handling, quarantine, backfills, orchestration and idempotency.

## Metrics
Do not represent synthetic demo results as production throughput. Measure data volume and elapsed runtime in a documented environment before quoting performance.
## Reproducible synthetic benchmark
Run `python -m scripts.benchmark` from the repository root. A local run on 2026-11-14 processed **22,400 synthetic CSV rows**, retained **20,000 distinct valid orders**, quarantined **400 malformed rows**, and deduplicated **2,000 repeats** in approximately **0.255 seconds** of local CSV/SQLite processing. Results depend on the machine and are **not Azure Databricks performance measurements**.
