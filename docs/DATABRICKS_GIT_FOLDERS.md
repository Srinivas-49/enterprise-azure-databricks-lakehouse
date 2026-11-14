# Workspace Git folder / notebook run sequence

**These paths are a proposed layout, not folders created in your workspace.**

1. Sign in to your actual Databricks Free Edition workspace (the URL is a Databricks domain, not an Azure Logic Apps trigger URL).
2. Workspace > choose your home folder > Create > Git folder.
3. Clone `https://github.com/Srinivas-49/enterprise-azure-databricks-lakehouse.git` using GitHub as the provider, and select `main`. If the UI requests Git credentials, configure Git provider credentials in Databricks settings rather than pasting a token into a notebook.
4. Run `databricks_free/01_end_to_end_lakehouse.py` to learn event ingestion, Delta MERGE, replay, quarantine and Gold tables.
5. Run `databricks_free/02_explore_results.sql` to explore Delta history and KPIs.
6. Run `databricks_free/03_multisource_spark_gold.py` to test multi-source Spark joins, exact integer-penny revenue, rejected records and customer segment analytics.
7. Screenshots to retain: notebook run result, Catalog Explorer tables, validation assertions, Gold outputs and Git commit SHA.

The notebook creates `portfolio_multisource` schema and Delta managed tables. It overwrites only tables in that schema, using deterministic **synthetic** data; do not point it at business datasets or shared production schemas. Tests in GitHub Actions inspect syntax/structure only; a green badge is not proof of Databricks runtime execution.

**No workspace creation, Azure resource deployment or Databricks authentication has happened through the ChatGPT GitHub integration.** Cloud workspace provisioning requires a supported authenticated action or a user-managed portal session.
