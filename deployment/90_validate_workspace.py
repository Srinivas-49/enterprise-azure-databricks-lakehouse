# Databricks notebook source
# MAGIC %md
# MAGIC # Cross-project quality gate — Delta table checks and reconciliation
# MAGIC Run after 01_end_to_end_lakehouse and 03_multisource_spark_gold.
# MAGIC The job fails on any violated contract. Counts apply to default fixture sizes.

# COMMAND ----------
from pyspark.sql import functions as F

# Verified on synthetic data generated inside the notebooks only.
expected = {
    "portfolio_lakehouse.bronze_order_events": 554,
    "portfolio_lakehouse.silver_orders": 500,
    "portfolio_lakehouse.silver_order_event_history": 551,
    "portfolio_lakehouse.silver_quarantine": 3,
    "portfolio_multisource.bronze_orders": 304,
    "portfolio_multisource.silver_orders": 300,
    "portfolio_multisource.silver_reconciled_orders": 300,
    "portfolio_multisource.silver_quarantine": 2,
}
observed = {}
for name, target in expected.items():
    actual = spark.table(name).count()
    observed[name] = actual
    assert actual == target, f"{name}: expected {target}, got {actual}"

# COMMAND ----------
silver = spark.table("portfolio_multisource.silver_reconciled_orders")
assert silver.groupBy("order_id").count().filter("count > 1").count() == 0
assert silver.filter("customer_id IS NULL OR payment_id IS NULL").count() == 0
assert silver.filter("amount_pennies <= 0").count() == 0
paid = F.when(F.col("status") == "paid", F.col("amount_pennies")).otherwise(F.lit(0))
source_amount = silver.agg(F.sum(paid)).first()[0]
by_day = spark.table("portfolio_multisource.gold_daily_revenue").agg(F.sum("paid_amount_pennies")).first()[0]
by_segment = spark.table("portfolio_multisource.gold_customer_segments").agg(F.sum("paid_amount_pennies")).first()[0]
assert source_amount == by_day == by_segment, "Gold revenue totals diverge"

# COMMAND ----------
# Persist the successful audit only AFTER all checks have passed.
audit_rows = [(name, rows) for name, rows in observed.items()]
audit = (spark.createDataFrame(audit_rows, "dataset string, row_count long")
         .withColumn("run_label", F.lit("synthetic-portfolio"))
         .withColumn("checked_at", F.current_timestamp())
         .withColumn("status", F.lit("passed"))
         .select("run_label", "checked_at", "dataset", "row_count", "status"))
audit.write.format("delta").mode("append").saveAsTable("portfolio_ops.quality_audit")
print("PASS:", len(observed), "Delta table count checks; uniqueness; referential integrity; revenue reconciliation")
display(audit.orderBy("dataset"))
