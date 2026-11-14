# Real public-data pipeline: NYC 311 + Open-Meteo

Official source:
- NYC Open Data 311, dataset ID `erm2-nwe9`: https://data.cityofnewyork.us/d/erm2-nwe9
- Open-Meteo historical archive: https://open-meteo.com/en/docs/historical-weather-api

The official NYC 311 dataset had approximately 23 million rows as of November 2026; this is **upstream availability**, not our executed volume. Open-Meteo's free API is for non-commercial use and requires attribution (CC BY 4.0). This is a learning project.

### Download a bounded real-world sample

    python -m scripts.ingest_public_311 --start 2025-01-01 --end 2025-01-03 --page-size 1000 --max-pages 2 --out raw-public

The script filters by date window, requests **only** unique key, creation/closure dates, agency, problem type, borough and status, and creates SHA-256 manifest sidecars. It excludes street addresses, coordinates and free-text descriptions. No API key is needed for a basic public request; optional Socrata app token can be supplied through SOCRATA_APP_TOKEN environment variable and is never written to disk. Weather is downloaded for the same period.

**Volume rules:** up to 31 days/window, up to 5000 records/page and 100 pages/invocation, with a boolean partial-window warning when the cap is reached. Each day/month can be run separately; do not claim a complete period if `partial_window_possible=true`. Offset pagination across a changing dataset may skip or repeat rows: reconcile on request ID and refresh prior windows.

### In Databricks

1. Clone this GitHub repo into your real Databricks workspace.
2. Create a **Unity Catalog Volume** that you own (in an existing writable catalog/schema). Upload the `nyc311/` and `weather/` folders under a path like `/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/public/`.
3. Open `databricks_public/01_nyc311_weather_lakehouse.py`, set the `landing_root` widget to that public landing path and run.
4. Inspect `portfolio_nyc311` managed tables: Bronze raw events; Silver validated requests, quarantine and weather; Gold agency/borough/problem/day service KPIs.
5. Capture **actual** row counts, invalid percentages, Delta history, duration and query plans. Scale to monthly windows only after small batches and cost limits pass.

### Professional upgrade

- Source watermarks and closed-record backfill refresh
- Retryable API ingestion with durable orchestrator/job checkpoints
- Optional ADF or Azure Functions scheduled ingestion from approved public APIs
- Data contract evolution and table owner tags
- Observability (freshness, duplicate rate, SLA), partition retention and Azure cost tracking

**Not deployed:** This repo changes source code only. GitHub CI validates tests; neither a live NYC API ingestion nor a Databricks job has been executed by the assistant.
