# REAL DATA: NYC 311 operational records + restaurant inspection citations

Sources (official NYC Open Data):
- 311 Service Requests (2020–present): https://data.cityofnewyork.us/d/erm2-nwe9 — approximately 23M rows at source inspection.
- Restaurant Inspection Results: https://data.cityofnewyork.us/d/43nn-pn8j — approximately 295K citation records. **Multiple citations may belong to one restaurant inspection.**

The pipeline fetches small paged snapshots using the **official public Socrata API** and stores each page as JSON Lines with an SHA-256 provenance manifest. The aggregate joins the two independently sourced datasets by **borough and month**, producing requests vs inspection activity. This is *ecological analysis*, not a causal relationship or restaurant-level linkage.

Run on your computer (not inside Databricks Free Edition, which restricts outbound internet):
```bash
python -m real_data.nyc_sources --pages 2 --page-size 1000 --after 2025-01-01 --output real-data/nyc
```
Then upload the landed JSONL pages to a Databricks Volume in your workspace and use Spark to compute Bronze→Silver→Gold. Do **not** commit downloaded datasets, especially record-level files, to GitHub.

## Scale-up controls
Use --pages N --page-size 5000 --confirm-large for carefully controlled larger backfills; run in date-sliced windows and verify 311 source watermark, completeness, schema changes and publication correction logs. Offset pagination across a changing dataset is NOT a consistent snapshot; manifests and checksums make each actual page reproducible. Real 23M-row processing needs suitable paid compute/storage and a separate cost review; no evidence of processing the entire source is claimed.

Fields intentionally selected exclude addresses, ZIP codes, exact coordinates, names or contact details. Public data may still need an appropriate privacy assessment before production publishing. In Free Edition keep batches small.

## Source licences, terms and attribution
Credit NYC Open Data / NYC 311 and NYC DOHMH. Check current dataset terms before redistribution; source licence may be unspecified. In this repository only download code and mock test records are committed.
