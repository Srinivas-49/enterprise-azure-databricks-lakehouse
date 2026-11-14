# Engineering performance and cost experiment plan

Included example code: conditional broadcast vs shuffle join decisions, month-based Delta partitioning, optional Databricks Z-ORDER optimization, and Terraform for an ADLS Gen2 lake.

For valid benchmarks capture baseline and optimized plans on identical datasets. Run at least five timings, recording query median and p95, cluster SKU, worker count, DBUs, data size, storage reads and partition counts. Benchmark join strategy, data layout and cluster sizing independently. Measure retries, data-quality rejection and recovery.

These techniques are examples. The repository has not measured cloud speedups, cost savings, TB-scale performance, SLAs or a production Azure deployment.
