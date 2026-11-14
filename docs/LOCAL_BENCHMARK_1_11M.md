# Reproducible local benchmark, 14 November 2026

**Scope:** Python CSV ingestion/validation/aggregation + SQLite serving from the local reference project archive. **Not Apache Spark, Azure Databricks or production.** No before/after Databricks join/partition/Z-ORDER results exist yet.

| Observed metric | Result |
| --- | ---: |
| Synthetic input orders | 1,110,000 |
| Distinct valid orders retained | 1,000,000 |
| Duplicate orders deduplicated | 100,000 |
| Malformed orders quarantined | 10,000 |
| Gold reporting dates | 28 |
| Local pipeline elapsed time | 8.1144 seconds |
| Local effective processing rate | 136,794 input rows/second |
| Data generation time (separate) | 1.4574 seconds |
| Peak resident memory | ~1.12 GiB |
| Execution environment | Linux, Python 3.13.5, single-process CSV/SQLite |

The command reproducing the dataset and pipeline is:

```bash
python -m scripts.benchmark_million --unique 1000000 --duplicates 100000 --invalid 10000
```

**Interpretation:** The observations demonstrate a local pipeline scale/reconciliation test only. The original run used the November 14 local project archive, prior to further cloud-adapter changes. Re-run the committed benchmark for a matching Git SHA and capture the result. The 50M Databricks benchmark in the Uber repository is a proposed cloud experiment; neither its runtime nor cost benefits have been measured.
