# Databricks notebook source
# MAGIC %md
# MAGIC # Real NYC Operational Lakehouse: 311 + restaurant inspections
# MAGIC Official public data. Import the paginated real-data files into a Unity Catalog
# MAGIC Volume first. Set landing_root to the Volume directory containing BOTH
# MAGIC service_requests/ and restaurant_inspections/ folders. No synthetic fallback.
# MAGIC Data is distributed by Spark; do not collect millions of records to the driver.

# COMMAND ----------
from pyspark.sql import functions as F, Window

dbutils.widgets.text("landing_root", "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/nyc")
root = dbutils.widgets.get("landing_root").rstrip("/")
if not root.startswith("/Volumes/") or "YOUR_" in root:
    raise ValueError("Upload source files to an actual managed Volume and set landing_root")
spark.sql("CREATE SCHEMA IF NOT EXISTS portfolio_nyc_real")
spark.sql("USE SCHEMA portfolio_nyc_real")

# COMMAND ----------
requests_raw = spark.read.json(f"{root}/service_requests/page_*.jsonl")
inspections_raw = spark.read.json(f"{root}/restaurant_inspections/page_*.jsonl")
for name, raw in (("bronze_311", requests_raw), ("bronze_restaurant_citations", inspections_raw)):
    if raw.isEmpty():
        raise ValueError(f"{name} is empty; check your file upload and source manifest")
    raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(name)

# COMMAND ----------
boroughs = ["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"]
def clean_borough(field):
    cleaned = F.upper(F.trim(F.col(field).cast("string")))
    return (F.when(cleaned == "1", "MANHATTAN").when(cleaned == "2", "BRONX")
            .when(cleaned == "3", "BROOKLYN").when(cleaned == "4", "QUEENS")
            .when(cleaned == "5", "STATEN ISLAND").otherwise(cleaned))

complaints = (requests_raw
    .withColumn("borough_clean", clean_borough("borough"))
    .withColumn("created_ts", F.expr("try_cast(created_date AS TIMESTAMP)"))
    .withColumn("closed_ts", F.expr("try_cast(closed_date AS TIMESTAMP)"))
    .withColumn("quality_reason", F.concat_ws("; ",
        F.when(F.col("unique_key").isNull() | (F.length(F.trim("unique_key")) == 0), "missing request key"),
        F.when(F.col("created_ts").isNull(), "invalid created date"),
        F.when(~F.col("borough_clean").isin(boroughs), "unrecognized borough")))
rejected_311 = complaints.filter(F.length("quality_reason") > 0)
valid_311 = complaints.filter(F.length("quality_reason") == 0)
ordering = Window.partitionBy("unique_key").orderBy(
    F.col("closed_ts").desc_nulls_last(), F.col("status").desc_nulls_last())
valid_311 = (valid_311.withColumn("rn", F.row_number().over(ordering))
             .filter("rn = 1").drop("rn", "quality_reason"))
rejected_311.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_311_quarantine")
valid_311.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_311_requests")

# COMMAND ----------
inspections = (inspections_raw
    .withColumn("borough_clean", clean_borough("boro"))
    .withColumn("inspection_ts", F.expr("try_cast(inspection_date AS TIMESTAMP)"))
    .withColumn("quality_reason", F.concat_ws("; ",
        F.when(F.col("camis").isNull() | (F.length(F.trim("camis")) == 0), "missing restaurant key"),
        F.when(F.col("inspection_ts").isNull(), "invalid inspection date"),
        F.when(F.year("inspection_ts") < 2000, "placeholder or ancient inspection date"),
        F.when(~F.col("borough_clean").isin(boroughs), "unrecognized borough")))
inspections.filter(F.length("quality_reason") > 0).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_restaurant_quarantine")
valid_restaurants = inspections.filter(F.length("quality_reason") == 0).drop("quality_reason")
valid_restaurants.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_restaurant_citations")

# COMMAND ----------
# Official inspection dataset is CITATION-GRAIN: one inspection can contain
# several violations. Inspect count uses distinct (camis, inspection_date).
request_kpis = (valid_311
    .withColumn("month", F.date_trunc("month", "created_ts").cast("date"))
    .withColumn("resolved_hours", F.when(F.col("closed_ts") >= F.col("created_ts"),
        (F.col("closed_ts").cast("long") - F.col("created_ts").cast("long")) / 3600))
    .groupBy("borough_clean", "month")
    .agg(F.count("*").alias("service_requests"),
         F.count("resolved_hours").alias("closed_valid_requests"),
         F.avg("resolved_hours").alias("mean_resolution_hours")))
inspection_kpis = (valid_restaurants
    .withColumn("month", F.date_trunc("month", "inspection_ts").cast("date"))
    .groupBy("borough_clean", "month")
    .agg(F.countDistinct(F.struct("camis", "inspection_date")).alias("distinct_restaurant_inspections"),
         F.sum(F.when(F.col("critical_flag") == "Critical", 1).otherwise(0)).alias("critical_violation_citations")))
joined = (request_kpis.alias("r").join(inspection_kpis.alias("i"),
    (F.col("r.borough_clean") == F.col("i.borough_clean")) & (F.col("r.month") == F.col("i.month")),
    "full_outer")
    .select(F.coalesce(F.col("r.borough_clean"), F.col("i.borough_clean")).alias("borough"),
            F.coalesce(F.col("r.month"), F.col("i.month")).alias("month"),
            F.coalesce("service_requests", F.lit(0)).alias("service_requests"),
            F.col("closed_valid_requests"),
            F.col("mean_resolution_hours"),
            F.coalesce("distinct_restaurant_inspections", F.lit(0)).alias("distinct_restaurant_inspections"),
            F.coalesce("critical_violation_citations", F.lit(0)).alias("critical_violation_citations")))
joined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_borough_month_operations")

# COMMAND ----------
bronze_n = requests_raw.count()
valid_n = valid_311.count()
invalid_n = rejected_311.count()
distinct_n = requests_raw.select("unique_key").distinct().count()
assert bronze_n >= valid_n + invalid_n
assert valid_n <= distinct_n
assert joined.filter("borough IS NULL OR month IS NULL").count() == 0
assert joined.filter("service_requests < 0 OR distinct_restaurant_inspections < 0").count() == 0
assert joined.agg(F.sum("service_requests")).first()[0] == valid_n
print("PASS: real NYC 311 requests", bronze_n, "valid unique", valid_n, "quality rejected", invalid_n)
print("NOTE: restaurant source is citation-grain, not one-record-per-inspection")
display(spark.table("gold_borough_month_operations").orderBy("month", "borough"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Engineering boundaries
# MAGIC Source datasets change over time. Offset-page backfills across live source
# MAGIC mutations are not transactionally consistent. For enterprise use replace with
# MAGIC date-partitioned immutable snapshots, versioned ingest manifests and CDC.
# MAGIC Compare borough-month aggregate trends; do not infer that service complaints
# MAGIC caused restaurant inspection outcomes. Publish only aggregated Gold outputs.
