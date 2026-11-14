# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace bootstrap — governed synthetic portfolio schemas
# MAGIC Uses your EXISTING Unity Catalog catalog. Does not create an account-level catalog.
# MAGIC Run with a user/service principal that has USE CATALOG and CREATE SCHEMA.

# COMMAND ----------
import re

catalog = spark.sql("SELECT current_catalog() AS catalog").first()["catalog"]
if not re.fullmatch(r"[A-Za-z0-9_]+", catalog):
    raise ValueError("Unexpected current catalog identifier")
if catalog == "hive_metastore":
    raise RuntimeError("Select a Unity Catalog catalog first; do not deploy to hive_metastore.")
print("Using existing catalog:", catalog)

schemas = {
    "portfolio_lakehouse": "Reproducible synthetic event CDC and Gold tables",
    "portfolio_multisource": "Three-source synthetic orders, payments and customer analytics",
    "portfolio_ops": "Synthetic pipeline quality evidence and audit history",
}

for schema, comment in schemas.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT '{comment}'")
    # Ordinary non-sensitive tags, NOT account-governed mandatory classifications.
    try:
        spark.sql(f"ALTER SCHEMA `{catalog}`.`{schema}` "
                  "SET TAGS ('project' = 'data_engineering_portfolio', "
                  "'data_classification' = 'synthetic', 'environment' = 'learning')")
        print("Schema and tags prepared:", schema)
    except Exception as exc:
        # Tag permissions differ from schema ownership. Missing tags must be
        # visible in the evidence; a production governance pipeline should fail.
        print("WARNING: schema exists but tags were not applied:", schema,
              type(exc).__name__, str(exc)[:240])

# COMMAND ----------
spark.sql("""
CREATE TABLE IF NOT EXISTS portfolio_ops.quality_audit (
  run_label STRING,
  checked_at TIMESTAMP,
  dataset STRING,
  row_count BIGINT,
  status STRING
) USING DELTA
""")
print("PASS: baseline catalog schemas and ops audit table available")
