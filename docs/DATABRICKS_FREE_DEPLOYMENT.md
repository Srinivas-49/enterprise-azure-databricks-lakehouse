# Databricks Free Edition — run the enterprise lakehouse from scratch

This is a **learning deployment** using synthetic events and managed Delta tables. It does not require a Microsoft Azure subscription or any Azure resource. The notebook has not been executed in a signed-in Databricks workspace by the repository authoring assistant; runtime verification is a separate step.

## 0. Understand the environment

The original Databricks Community Edition was retired. Use **Databricks Free Edition**: https://www.databricks.com/learn/free-edition

Free Edition is a no-cost, serverless-only, quota-limited environment. Jobs and Lakeflow pipelines are supported within published limits, but custom clusters, custom workspace storage, compliance enforcement and private networking are not. It is **not** approved for production or commercial workloads. See https://docs.databricks.com/aws/en/getting-started/free-edition-limitations .

## 1. Import the GitHub repository

1. Sign in to your personal Databricks Free Edition workspace.
2. In the workspace UI, create a **Git folder** (or import a notebook if your UI lacks that option). Use this public URL:
   https://github.com/Srinivas-49/enterprise-azure-databricks-lakehouse
3. Select main and open `databricks_free/01_end_to_end_lakehouse.py` as a notebook. Alternatively import that Python notebook source file into Workspace.
4. Select **serverless** compute. Do not create a paid Azure workspace for this demonstration.
5. Keep `BASE_ORDERS=500`. The notebook creates `portfolio_lakehouse` schema in the workspace's current catalog. If you lack CREATE SCHEMA permission, ask the workspace owner or use a schema where you have CREATE TABLE permission.
6. Click **Run all**. Then run `databricks_free/02_explore_results.sql`.

## 2. Expected results

The demo generates 500 initial orders, two exact duplicate delivery attempts, three invalid records, 50 newer updates/refunds and one older late event. Deduplicated Delta Bronze should have **554** events, Silver current snapshot **500** orders, Silver event-level history **551** events and quarantine **3** events. The notebook asserts these values, verifies refund and late-event ordering, and replays all batches to check idempotence.

Expected managed Delta objects:
- `bronze_order_events`: raw JSON payloads and ingestion metadata
- `silver_orders`: latest valid order state by event timestamp and deterministic tie breaker
- `silver_order_event_history`: accepted event versions
- `silver_quarantine`: failed validation and raw evidence
- `gold_daily_revenue`, `gold_customer_360`, `gold_order_status`: analytics outputs
- `ops_pipeline_audit`: counts for run verification

Run twice to demonstrate rerun-safe ingestion; the Gold tables are full snapshot recalculations rather than incremental streaming materialisations. Use only synthetic records.

## 3. What makes this a serious portfolio demo?

- Spark-native sample data generation (no driver-side million-row Python list).
- Event ID deduplication in Bronze, Delta MERGE CDC upsert in Silver, timestamp-based late-event handling.
- Validation and quarantine for malformed JSON, missing identifiers and invalid amounts.
- Accepted event history and latest-state table, distinct from naive insert-only ETL.
- Three Gold analytics marts; single-row operational audit and explicit replay/invariant checks.
- Reproducible code, CI, design limitations and a documented upgrade path.

## 4. Move from Free Edition to a real Azure Databricks setup

In an independently provisioned, paid Azure environment, add ADLS Gen2 storage, Azure Data Factory/Auto Loader ingestion, Event Hubs or Kafka with durable checkpoints, separate dev/prod Unity Catalog permissions, managed identity, monitoring, alerts, scheduled jobs, Terraform and CI/CD deployment. These integrations are **not** implied by running the Free Edition notebook. Carry out cost and security reviews before provisioning any paid service.

## 5. Trouble-shooting

- **Schema creation error:** confirm current catalog via `SELECT current_catalog()`; check CREATE SCHEMA/CREATE TABLE grants. Use an existing writable schema if necessary.
- **Serverless limit exceeded:** wait for quota reset, and lower `BASE_ORDERS` (the built-in assertions adapt to the size).
- **MERGE permission error:** managed Delta tables must be writable; confirm you are using a schema you own.
- **Unexpected counts:** this runbook assumes clean tables or a previous run using the exact same deterministic event IDs. For new experiments, create a new schema rather than destroying unrelated work.
- **Git folder unavailable:** import the notebook directly from its GitHub `.py` source.

## 6. Security

Do not paste passwords, personal access tokens or connection strings into GitHub, ChatGPT or notebook source. The free tutorial does not need credentials beyond the workspace login.
