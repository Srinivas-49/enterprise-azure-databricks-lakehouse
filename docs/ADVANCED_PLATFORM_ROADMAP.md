# From Free Edition notebook to production Azure data platform

This is an engineering roadmap, not a claim that the infrastructure is deployed.

| Capability | Free Edition demo | Production design |
| --- | --- | --- |
| Source ingestion | Deterministic synthetic batches | ADF, Auto Loader, CDC feeds, managed service identity |
| Raw retention | Delta Bronze events | Versioned raw storage with retention and privacy policy |
| Data quality | Schema, null, amount, enum quarantine | Contracts, thresholds, event log, alerting and steward workflow |
| Reliability | Delta MERGE, replay, timestamp ordering | Checkpoints, bounded retries, DLQ, reconciliation, rollback |
| Incremental state | Event history and current-order snapshot | Change data feed, SCD2 history and late-data policy |
| Analytics | Revenue, status and customer aggregates | Star schema with fact orders, dim customer/date/product |
| Governance | Managed tables | Unity Catalog least privilege, masking, classifications, lineage |
| Testing | Offline Python tests plus notebook assertions | Spark integration CI, data contract/CDC tests, environment smoke tests |
| Operations | Audit counts in a Delta table | Job telemetry, service SLOs, pipeline error budget and runbooks |
| Deployment | Manual Git-folder notebook run | Declarative Automation Bundles and GitHub Actions with OIDC |
| Performance | Small learning data | Measured partitions, skew, shuffle, DBU and capacity benchmarks |

## Evidence discipline

Do not claim a production-scale run, SLA, 1M-row Spark performance or real customer workload from this notebook. The earlier 1.11M synthetic benchmark in the repository is a **local Python/SQLite measurement** and should not be relabelled as Databricks throughput.

## Recommended implementation milestones

1. **Free Edition**: execute notebook, save query outputs and attach screenshots of actual Delta table counts.
2. **Free Edition**: experiment with a larger Spark-generated synthetic batch, rerun and compare runtime without exceeding quotas.
3. **Cloud trial/paid Azure**: choose a budget and create a separate secured storage and catalog environment.
4. **Professional deployment**: split notebook functions into packages, add integration tests, job bundles and service-identity authentication.
5. **Production readiness**: implement source contracts, PII classification, lineage, access reviews, failure injection, monitoring and measurable SLOs.
