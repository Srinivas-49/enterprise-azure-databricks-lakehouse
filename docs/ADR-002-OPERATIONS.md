# ADR-002 — Engineering decisions and operational acceptance

## Why Airflow / ADF?
Use Airflow if there is cross-platform dependency orchestration and Python-based workflows. Use Azure Data Factory for managed Azure source movement and scheduling. Avoid double-scheduling the same job without run coordination.

## Why Terraform?
Versioned, reviewable infrastructure plans for ADLS Gen2 resources. Production deployments need remote state, OIDC, environment boundaries, minimum RBAC and cost preapproval.

## Minimum acceptance tests
- Input contract and schema drift tests
- Duplicate input replay, idempotent rerun and quarantine
- SQL Gold reconciliation against an independent expected-value query
- REST retry/backoff, pagination and rate-limits before production usage
- Kafka consumer restart, offset validation and poison-message routing
- Airflow retries, task timeouts and alert firing
- Unity Catalog grants, masked data, lineage, and access control review
- Databricks Spark plan/cluster comparison plus DBUs and cost/row

## Evidence checklist
Run ID, Git SHA, dataset size, row counts, failure cases, pytest report, Spark UI metrics, p50/p95 duration, compute SKU, DBUs, cost and reviewer feedback; link evidence in repo. Don't report green from unrun tests.
