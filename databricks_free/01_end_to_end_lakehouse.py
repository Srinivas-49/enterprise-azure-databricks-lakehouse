# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Lakehouse | Databricks Free Edition
# MAGIC **Bronze -> Silver -> Gold** with deterministic synthetic data, Delta MERGE,
# MAGIC event-level history, quality quarantine, replay checks and metrics.
# MAGIC Runs on managed tables in the current workspace catalog; no Azure resources needed.
# MAGIC Not validated in an authenticated Databricks workspace yet.

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType
)
import re

# Keep this small on Free Edition. Increase only after the end-to-end run succeeds.
BASE_ORDERS = 500
SCHEMA_NAME = "portfolio_lakehouse"
assert 50 <= BASE_ORDERS <= 100_000
assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", SCHEMA_NAME)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")
print("Catalog:", spark.sql("SELECT current_catalog()").first()[0], "Schema:", SCHEMA_NAME)

# COMMAND ----------
# All test records are synthetic. This generates events in Spark, not Python-driver lists.
def order_events(ids, batch_id, event_time, override_status=None):
    base = (
        ids.withColumn("order_id", F.concat(F.lit("ord-"), F.lpad(F.col("id").cast("string"), 7, "0")))
        .withColumn("customer_id", F.concat(F.lit("cust-"), (F.col("id") % 100).cast("string")))
        .withColumn("order_date", F.date_format(
            F.date_add(F.lit("2026-11-01").cast("date"), (F.col("id") % 14).cast("int")), "yyyy-MM-dd"))
        .withColumn("amount", F.concat(((F.col("id") % 200) + 5).cast("string"), F.lit(".50")))
        .withColumn("status", F.lit(override_status) if override_status is not None
                    else F.when(F.col("id") % 5 == 0, "pending").otherwise("paid"))
        .withColumn("updated_at", F.lit(event_time))
        .withColumn("event_id", F.concat(F.lit(batch_id + "-"), F.lpad(F.col("id").cast("string"), 7, "0")))
    )
    return base.select(
        "event_id", F.lit(batch_id).alias("batch_id"),
        F.to_json(F.struct("order_id", "customer_id", "order_date",
                           "amount", "status", "updated_at")).alias("payload")
    )

initial = order_events(spark.range(BASE_ORDERS), "seed", "2026-11-15 09:00:00")
# Exact duplicate event IDs simulate at-least-once delivery.
duplicates = initial.filter(F.col("event_id").isin("seed-0000000", "seed-0000001"))
invalid_events = spark.createDataFrame([
    ("bad-amount", "seed", '{"order_id":"bad-1","customer_id":"c","order_date":"2026-11-01","amount":"-2.00","status":"paid","updated_at":"2026-11-15 09:00:00"}'),
    ("bad-key", "seed", '{"order_id":"","customer_id":"c","order_date":"2026-11-01","amount":"4.00","status":"paid","updated_at":"2026-11-15 09:00:00"}'),
    ("bad-json", "seed", '{"order_id":')
], ["event_id", "batch_id", "payload"])
seed_batch = initial.unionByName(duplicates).unionByName(invalid_events)

# 10% of orders receive a newer refund event. One older late arrival must not
# overwrite the current state. The event history still retains that older event.
change_batch = order_events(spark.range(0, BASE_ORDERS, 10),
                            "change", "2026-11-16 09:00:00", "refunded")
late_batch = order_events(spark.range(5, 6),
                          "late", "2026-11-14 09:00:00", "cancelled")

# COMMAND ----------
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_order_events (
    event_id STRING, batch_id STRING, payload STRING, ingested_at TIMESTAMP
) USING DELTA
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id STRING, customer_id STRING, order_date DATE,
    amount DECIMAL(18,2), status STRING, updated_at TIMESTAMP,
    source_event_id STRING, source_batch STRING
) USING DELTA
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_order_event_history (
    order_id STRING, customer_id STRING, order_date DATE,
    amount DECIMAL(18,2), status STRING, updated_at TIMESTAMP,
    source_event_id STRING, source_batch STRING
) USING DELTA
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_quarantine (
    event_id STRING, batch_id STRING, order_id STRING,
    rejection_reason STRING, raw_payload STRING
) USING DELTA
""")
spark.sql("""
CREATE TABLE IF NOT EXISTS ops_pipeline_audit (
    batch_id STRING, bronze_rows BIGINT, silver_orders BIGINT,
    quarantined_events BIGINT, checked_at TIMESTAMP
) USING DELTA
""")

EVENT_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_date", StringType()),
    StructField("amount", StringType()),
    StructField("status", StringType()),
    StructField("updated_at", StringType()),
])
SILVER_COLUMNS = ["order_id", "customer_id", "order_date", "amount",
                  "status", "updated_at", "source_event_id", "source_batch"]

# COMMAND ----------
def process_batch(batch_id, source):
    """Idempotent ingest by event ID; latest-by-event-time order snapshot.

    Deliberately no external checkpoints or exactly-once Kafka claims. This is a
    bounded Databricks Delta notebook reference for synthetic event batches.
    """
    incoming = source.dropDuplicates(["event_id"]).withColumn("ingested_at", F.current_timestamp())
    incoming.createOrReplaceTempView("incoming_events")
    spark.sql("""
        MERGE INTO bronze_order_events AS t USING incoming_events AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    decoded = (spark.table("bronze_order_events")
        .filter(F.col("batch_id") == batch_id)
        .withColumn("decoded", F.from_json("payload", EVENT_SCHEMA))
        .select("event_id", "batch_id", "payload", "decoded.*")
        .withColumn("parsed_date", F.expr("try_cast(order_date AS DATE)"))
        .withColumn("parsed_amount", F.expr("try_cast(amount AS DECIMAL(18,2))"))
        .withColumn("parsed_timestamp", F.expr("try_cast(updated_at AS TIMESTAMP)"))
    )
    reason = F.concat_ws("; ",
        F.when(F.col("order_id").isNull() | (F.length(F.trim("order_id")) == 0), "missing order_id"),
        F.when(F.col("customer_id").isNull() | (F.length(F.trim("customer_id")) == 0), "missing customer_id"),
        F.when(F.col("parsed_date").isNull(), "invalid order_date"),
        F.when(F.col("amount").isNull() | (~F.col("amount").rlike(r"^[0-9]{1,12}(\.[0-9]{1,2})?$"))
               | F.col("parsed_amount").isNull() | (F.col("parsed_amount") <= 0), "invalid amount"),
        F.when(F.col("status").isNull() | (~F.col("status").isin("paid", "pending", "refunded", "cancelled")), "invalid status"),
        F.when(F.col("parsed_timestamp").isNull(), "invalid updated_at")
    )
    scored = decoded.withColumn("rejection_reason", reason)

    rejected = (scored.filter(F.length("rejection_reason") > 0)
        .select("event_id", "batch_id", "order_id", "rejection_reason",
                F.col("payload").alias("raw_payload")))
    rejected.createOrReplaceTempView("rejected_events")
    spark.sql("""
        MERGE INTO silver_quarantine AS t USING rejected_events AS s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    accepted = (scored.filter(F.length("rejection_reason") == 0)
        .select("order_id", "customer_id",
                F.col("parsed_date").alias("order_date"),
                F.col("parsed_amount").alias("amount"),
                "status", F.col("parsed_timestamp").alias("updated_at"),
                F.col("event_id").alias("source_event_id"),
                F.col("batch_id").alias("source_batch")))

    # Event-level history preserves accepted changes, even late arrivals.
    accepted.createOrReplaceTempView("accepted_events")
    spark.sql("""
        MERGE INTO silver_order_event_history AS t USING accepted_events AS s
        ON t.source_event_id = s.source_event_id
        WHEN NOT MATCHED THEN INSERT *
    """)

    order_window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc(), F.col("source_event_id").desc())
    newest = (accepted.withColumn("_rank", F.row_number().over(order_window))
              .filter(F.col("_rank") == 1).select(*SILVER_COLUMNS))
    newest.createOrReplaceTempView("latest_accepted_orders")
    spark.sql("""
        MERGE INTO silver_orders AS t USING latest_accepted_orders AS s
        ON t.order_id = s.order_id
        WHEN MATCHED AND (
            s.updated_at > t.updated_at
            OR (s.updated_at = t.updated_at AND s.source_event_id > t.source_event_id)
        ) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Processed batch", batch_id)

# COMMAND ----------
# Ingest original delivery, update events, and a late arrival.
process_batch("seed", seed_batch)
process_batch("change", change_batch)
process_batch("late", late_batch)

# COMMAND ----------
# Gold is rebuilt from the current Silver snapshot each run. This is intentional
# for reproducible learning; production scale may require incremental Gold updates.
spark.sql("""
CREATE OR REPLACE TABLE gold_daily_revenue USING DELTA AS
SELECT order_date,
       COUNT(*) AS total_orders,
       SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
       SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders,
       CAST(SUM(CASE WHEN status = 'paid' THEN amount
                     ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(20,2)) AS paid_revenue
FROM silver_orders GROUP BY order_date
""")
spark.sql("""
CREATE OR REPLACE TABLE gold_customer_360 USING DELTA AS
SELECT customer_id,
       COUNT(*) AS total_orders,
       SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS paid_orders,
       MIN(order_date) AS first_order_date,
       MAX(order_date) AS last_order_date,
       CAST(SUM(CASE WHEN status = 'paid' THEN amount
                     ELSE CAST(0 AS DECIMAL(18,2)) END) AS DECIMAL(20,2)) AS lifetime_paid_revenue
FROM silver_orders GROUP BY customer_id
""")
spark.sql("""
CREATE OR REPLACE TABLE gold_order_status USING DELTA AS
SELECT status, COUNT(*) AS order_count FROM silver_orders GROUP BY status
""")

# COMMAND ----------
# Conclusive checks for this deterministic synthetic dataset.
expected_history = BASE_ORDERS + len(range(0, BASE_ORDERS, 10)) + 1
assert spark.table("silver_orders").count() == BASE_ORDERS
assert spark.table("silver_order_event_history").count() == expected_history
assert spark.table("silver_quarantine").count() == 3
assert spark.table("bronze_order_events").count() == expected_history + 3
assert spark.table("silver_orders").filter("order_id = 'ord-0000000' AND status = 'refunded'").count() == 1
assert spark.table("silver_orders").filter("order_id = 'ord-0000005' AND status = 'cancelled'").count() == 0

# Replay all batches: Delta MERGE must not double-count source events or orders.
process_batch("seed", seed_batch)
process_batch("change", change_batch)
process_batch("late", late_batch)
assert spark.table("bronze_order_events").count() == expected_history + 3
assert spark.table("silver_orders").count() == BASE_ORDERS
assert spark.table("silver_order_event_history").count() == expected_history
print("PASS: valid snapshot, quality quarantine, CDC ordering, late events, and replay")

# COMMAND ----------
from pyspark.sql.types import LongType, TimestampType
bronze_count = spark.table("bronze_order_events").count()
silver_count = spark.table("silver_orders").count()
quarantine_count = spark.table("silver_quarantine").count()
audit = spark.createDataFrame(
    [("demo-full-run", bronze_count, silver_count, quarantine_count)],
    "batch_id string, bronze_rows long, silver_orders long, quarantined_events long"
).withColumn("checked_at", F.current_timestamp())
audit.createOrReplaceTempView("incoming_audit")
spark.sql("""
MERGE INTO ops_pipeline_audit AS t USING incoming_audit AS s ON t.batch_id = s.batch_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(spark.table("gold_daily_revenue").orderBy("order_date"))
display(spark.table("gold_customer_360").orderBy(F.col("lifetime_paid_revenue").desc()).limit(20))
display(spark.table("ops_pipeline_audit"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## What to show in a portfolio demo
# MAGIC - Inspect Bronze raw events and the three rejected malformed/invalid records.
# MAGIC - Show that order ord-0000000 is refunded after a newer event.
# MAGIC - Show that an older late-arriving update does not overwrite order ord-0000005.
# MAGIC - Run the notebook twice; counts remain stable because ingestion uses Delta MERGE.
# MAGIC - Explore the three Gold tables and ops audit table in Catalog Explorer.
# MAGIC **Not included:** production Kafka, checkpoints, multi-tenant security, SLA, actual cloud benchmark.
