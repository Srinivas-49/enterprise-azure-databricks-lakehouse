# Databricks Free Edition: catalog, notebooks, tags and paused job

## Access and permission boundary
The previously shared URL points to an Azure Logic Apps signed HTTP endpoint, not a Databricks workspace. Revoke/rotate that signed trigger key. Never share workspace access tokens or passwords.

Use the actual Databricks workspace base URL (normally a databricks.com or azuredatabricks.net host with no signed query parameters). No cloud workspace has been accessed or modified by the authoring assistant.

Free Edition has usage quotas and some administrative limits; start with the workspace's existing writable Unity Catalog catalog. No catalog or schema is created merely by committing these files.

## Manual run order
1. In Databricks Workspace, create a Git folder from https://github.com/Srinivas-49/enterprise-azure-databricks-lakehouse.git (branch main).
2. Open **deployment/00_bootstrap_catalog.py**. Select your writable catalog and run it to create schemas, then inspect the output for any tag permission warnings.
3. Run **databricks_free/01_end_to_end_lakehouse.py** (Delta MERGE and replay).
4. Run **databricks_free/03_multisource_spark_gold.py** (three-source joins and Gold reconciliation).
5. Run **deployment/90_validate_workspace.py** (counts, uniqueness, quality and audit evidence).
6. If you have APPLY TAG permission, run **governance/02_apply_table_tags.sql** after the tables exist. In an admin-managed paid environment, the optional admin catalog definition is in **governance/01_admin_catalog_optional.sql**.
7. Capture job run IDs, executed timestamp, screenshots of Gold and quarantine tables, Delta history and the audit evidence. Do not report cloud success until the run passes.

## Declarative Automation Bundle
The repository root contains **databricks.yml** defining a four-task DAG:
bootstrap → CDC Bronze/Silver/Gold → multi-source Gold → cross-table validation.

The daily schedule is **PAUSED** and job concurrency is 1. No paid jobs are deployed or run from GitHub CI.

From your own authenticated terminal:
    databricks auth login --host https://YOUR-DATABRICKS-WORKSPACE
    databricks bundle validate --target dev
    databricks bundle deploy --target dev
    databricks bundle run portfolio_lakehouse --target dev

Validate can fail if workspace lacks privileges or support for a bundled job. Use the manual notebook path as a fallback. A bundle deploy or run is not automatically free on a paid Azure workspace; check billing before enabling any job.

## Operational hardening backlog
Replace hard-coded fixture sizes with job parameters; add a staging catalog, authoritative CDC sequencing, full schema contracts, SCD Type 2 for customers, column masking and row filtering, platform audit logs, failure/retry injection, cluster benchmarks, CI-to-workspace OIDC, and budgets/alerts in Azure before production.
