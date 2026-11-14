# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced PySpark: orders + payments + customers -> governed Gold
# MAGIC **Learning notebook.** Synthetic data only; no Azure credentials or external calls.
# MAGIC Run all cells in Databricks Free Edition serverless.
# MAGIC Workflow: generate -> Bronze -> quality/quarantine -> Silver -> dimension joins -> Gold -> reconciliation.

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import DecimalType

SCHEMA_NAME = "portfolio_multisource"
ORDER_COUNT = 300  # Keep small to stay inside Free Edition fair-use limits.
assert 10 <= ORDER_COUNT <= 10000
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

# COMMAND ----------
# Dimension attributes are deterministic; never use customer PII in a public demo.
customers = (
    spark.range(50)
    .select(
        F.format_string("C%04d", F.col("id")).alias("customer_id"),
        F.when(F.col("id") % 3 == 0, "business")
         .when(F.col("id") % 3 == 1, "plus").otherwise("standard").alias("segment"),
        F.lit("GB").alias("country")
    )
)
base = (
    spark.range(ORDER_COUNT)
    .select(
        F.format_string("O%06d", F.col("id")).alias("order_id"),
        F.format_string("C%04d", F.col("id") % 50).alias("customer_id"),
        F.date_add(F.lit("2026-11-01").cast("date"), (F.col("id") % 14).cast("int")).alias("order_date"),
        # Amount is stored as exact integer pennies.
        ((F.col("id") % 500) + 1000).cast("long").alias("amount_pennies"),
        F.when(F.col("id") % 5 == 0, "pending").otherwise("paid").alias("status"),
        F.lit("2026-11-14T09:00:00Z").alias("event_time"),
        F.format_string("order-event-%06d", F.col("id")).alias("event_id")
    )
)
dupes = base.filter(F.col("order_id").isin("O000000", "O000001"))
bad_missing_customer = (
    base.filter(F.col("order_id") == "O000002")
    .withColumn("customer_id", F.lit(None).cast("string"))
    .withColumn("event_id", F.lit("bad-missing-customer"))
)
bad_negative_amount = (
    base.filter(F.col("order_id") == "O000003")
    .withColumn("amount_pennies", F.lit(-100).cast("long"))
    .withColumn("event_id", F.lit("bad-negative-amount"))
)
orders_raw = base.unionByName(dupes).unionByName(bad_missing_customer).unionByName(bad_negative_amount)
payments_raw = (
    base.select(
        F.concat(F.lit("P-"), F.col("order_id")).alias("payment_id"),
        "order_id", "amount_pennies", "status"
    )
)

# COMMAND ----------
# Deterministic overwrite is intentional for reproducible portfolio testing.
# This notebook demonstrates batch ingestion; it is NOT a streaming source.
orders_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_orders")
customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_customers")
payments_raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_payments")

# COMMAND ----------
bronze = spark.table("bronze_orders")
violations = F.concat_ws("; ",
    F.when(F.col("order_id").isNull() | (F.length(F.trim("order_id")) == 0), "order_id missing"),
    F.when(F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0), "customer_id missing"),
    F.when(F.col("order_date").isNull(), "order_date missing"),
    F.when(F.col("amount_pennies").isNull() | (F.col("amount_pennies") <= 0), "invalid amount"),
    F.when(~F.col("status").isin("paid", "pending", "refunded"), "invalid status"),
    F.when(F.to_timestamp("event_time").isNull(), "invalid timestamp")
)
tagged = bronze.withColumn("quality_issue", violations)
rejected = tagged.filter(F.length("quality_issue") > 0)
accepted = tagged.filter(F.length("quality_issue") == 0).drop("quality_issue")
rejected.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_quarantine")
# Deterministic event ID ordering in case of same event timestamp.
rank = Window.partitionBy("order_id").orderBy(F.to_timestamp("event_time").desc(), F.col("event_id").desc())
silver = (
    accepted.withColumn("_rank", F.row_number().over(rank))
    .filter(F.col("_rank") == 1).drop("_rank")
)
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")

# COMMAND ----------
# Validate referential and monetary integrity before delivering analytics.
customer_dim = spark.table("bronze_customers")
payment_fact = spark.table("bronze_payments")
orders = spark.table("silver_orders")
payment_named = payment_fact.select(
    "order_id", "payment_id",
    F.col("amount_pennies").alias("payment_amount_pennies"),
    F.col("status").alias("payment_status")
)
with_customers = orders.join(F.broadcast(customer_dim), on="customer_id", how="left")
with_payments = with_customers.join(payment_named, on="order_id", how="left")
issues = with_payments.filter(
    F.col("segment").isNull()
    | F.col("payment_id").isNull()
    | (F.col("amount_pennies") != F.col("payment_amount_pennies"))
    | (F.col("status") != F.col("payment_status"))
)
assert issues.count() == 0, "Missing customer/payment or mismatched amounts/statuses"

# Explicit projection keeps the trusted Gold schema unambiguous.
trusted = with_payments.select(
    "order_id", "customer_id", "order_date", "amount_pennies",
    "status", "segment", "payment_id"
)
trusted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_reconciled_orders")

# COMMAND ----------
# Customer-segment Gold model with integer-currency arithmetic.
paid = F.when(F.col("status") == "paid", F.col("amount_pennies")).otherwise(F.lit(0))
gold_segments = (
    trusted.groupBy("segment")
    .agg(F.count("*").alias("total_orders"),
         F.sum(F.when(F.col("status") == "paid", 1).otherwise(0)).alias("paid_orders"),
         F.sum(paid).alias("paid_amount_pennies"))
    .withColumn("paid_revenue_gbp", (F.col("paid_amount_pennies") / F.lit(100)).cast(DecimalType(18, 2)))
)
gold_daily = (
    trusted.groupBy("order_date")
    .agg(F.count("*").alias("orders"),
         F.sum(paid).alias("paid_amount_pennies"))
    .withColumn("paid_revenue_gbp", (F.col("paid_amount_pennies") / F.lit(100)).cast(DecimalType(18, 2)))
)
gold_segments.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_customer_segments")
gold_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_daily_revenue")

# COMMAND ----------
counts = {
    "bronze_orders": spark.table("bronze_orders").count(),
    "quarantine": spark.table("silver_quarantine").count(),
    "silver_orders": spark.table("silver_orders").count(),
    "reconciled_orders": spark.table("silver_reconciled_orders").count(),
}
assert counts == {
    "bronze_orders": ORDER_COUNT + 4,
    "quarantine": 2,
    "silver_orders": ORDER_COUNT,
    "reconciled_orders": ORDER_COUNT,
}, counts
source_total = trusted.agg(F.sum(paid).alias("total")).first()["total"]
by_day = gold_daily.agg(F.sum("paid_amount_pennies").alias("total")).first()["total"]
by_segment = gold_segments.agg(F.sum("paid_amount_pennies").alias("total")).first()["total"]
assert source_total == by_day == by_segment, "Gold financial reconciliation failed"
print("PASS: source to Gold tests", counts, "paid revenue pennies", source_total)
display(spark.table("gold_customer_segments").orderBy("segment"))
display(spark.table("gold_daily_revenue").orderBy("order_date"))

# COMMAND ----------
# MAGIC %md
# MAGIC **Design boundaries:** The example uses synthetic local-to-workspace Spark data,
# MAGIC overwrites managed Delta tables on each run, and validates joins/revenue.
# MAGIC It does not claim live Azure ADF, Auto Loader, Kafka streaming, SCD2, or
# MAGIC production security. Extend with DLT/Lakeflow or scheduled jobs only after
# MAGIC runtime and budget review.
