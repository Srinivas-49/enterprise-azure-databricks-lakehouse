# Databricks notebook source
# MAGIC %md
# MAGIC # Real NYC 311 + historical weather | Bronze -> Silver -> Gold
# MAGIC 311: official NYC Open Data; weather: Open-Meteo (CC BY 4.0).
# MAGIC Download externally with scripts/ingest_public_311.py, then upload the
# MAGIC raw-public/nyc311 and raw-public/weather folders to a UC Volume.
# MAGIC No raw addresses, geographic coordinates or resident free text are ingested.
# MAGIC This is a batch refresh with latest-arrival MERGE, not authoritative source CDC.

# COMMAND ----------
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

dbutils.widgets.text("landing_root", "")
landing_root = dbutils.widgets.get("landing_root").rstrip("/")
if not landing_root.startswith("/Volumes/"):
    raise ValueError("Specify a Unity Catalog Volume path, e.g. /Volumes/<catalog>/<schema>/<volume>/public")

spark.sql("CREATE SCHEMA IF NOT EXISTS portfolio_nyc311")
spark.sql("USE SCHEMA portfolio_nyc311")

# COMMAND ----------
schema = StructType([StructField(name, StringType()) for name in
    ("unique_key", "created_date", "closed_date", "agency", "complaint_type", "borough", "status")])
raw = (spark.read.schema(schema)
       .json(f"{landing_root}/nyc311/*.jsonl")
       .withColumn("source_file", F.input_file_name()))
assert raw.limit(1).count() == 1, "Upload at least one public 311 JSONL file"
raw.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_requests")

# COMMAND ----------
parsed = (raw
    .withColumn("created_at", F.to_timestamp("created_date"))
    .withColumn("closed_at", F.to_timestamp("closed_date"))
    .withColumn("request_id", F.trim("unique_key"))
    .withColumn("agency_code", F.upper(F.trim("agency")))
    .withColumn("borough_name", F.upper(F.trim("borough")))
    .withColumn("problem", F.trim("complaint_type"))
    .withColumn("service_status", F.trim("status")))
reason = F.concat_ws("; ",
    F.when(F.col("request_id").isNull() | (F.length("request_id") == 0), "missing request ID"),
    F.when(F.col("created_at").isNull(), "invalid created date"),
    F.when(F.col("agency_code").isNull() | (F.length("agency_code") == 0), "missing agency"),
    F.when(F.col("closed_at") < F.col("created_at"), "closed before created"))
tagged = parsed.withColumn("rejection_reason", reason)
bad = tagged.filter(F.length("rejection_reason") > 0)
accepted = tagged.filter(F.length("rejection_reason") == 0)
bad.select("request_id", "source_file", "rejection_reason").write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable("silver_quarantine")

# Reconcile repeated reads / date partitions by stable NYC unique_key.
# This is a **snapshot merge**, not guaranteed ordering for concurrent API updates.
rank = Window.partitionBy("request_id").orderBy(F.col("closed_at").desc_nulls_last(),
                                                 F.col("created_at").desc(), F.col("source_file").desc())
latest = (accepted.withColumn("rn", F.row_number().over(rank))
          .where("rn=1").select("request_id", "created_at", "closed_at", "agency_code",
              "borough_name", "problem", "service_status", "source_file"))
latest.createOrReplaceTempView("latest_requests")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_requests (
 request_id STRING, created_at TIMESTAMP, closed_at TIMESTAMP, agency_code STRING,
 borough_name STRING, problem STRING, service_status STRING, source_file STRING
) USING DELTA
""")
spark.sql("""
MERGE INTO silver_requests t USING latest_requests s ON t.request_id=s.request_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
weather_schema = StructType([
    StructField("date", StringType()), StructField("temperature_mean_c", DoubleType()),
    StructField("precip_mm", DoubleType()), StructField("city", StringType()),
    StructField("source", StringType())
])
weather = (spark.read.schema(weather_schema).json(f"{landing_root}/weather/*.jsonl")
    .withColumn("service_date", F.to_date("date"))
    .select("service_date", "temperature_mean_c", "precip_mm")
    .dropDuplicates(["service_date"]))
weather.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_weather")

# REQUESTS INCLUDE NY LOCAL-TIME TIMESTAMPS; check this assumption with API data dictionary.
requests = (spark.table("silver_requests")
    .withColumn("service_date", F.to_date("created_at"))
    .withColumn("resolution_hours",
       (F.unix_timestamp("closed_at") - F.unix_timestamp("created_at")) / F.lit(3600.0)))
daily = (requests.groupBy("service_date", "borough_name", "agency_code", "problem")
    .agg(F.count("*").alias("requests"),
         F.sum(F.when(F.col("closed_at").isNotNull(), 1).otherwise(0)).alias("closed_requests"),
         F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours")))
gold = daily.join(F.broadcast(weather), "service_date", "left")
gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_service_operations")

# COMMAND ----------
assert spark.table("silver_requests").groupBy("request_id").count().filter("count > 1").count() == 0
assert requests.count() >= gold.agg(F.sum("requests")).first()[0]
assert spark.table("silver_quarantine").filter("rejection_reason IS NULL").count() == 0
print("PASS: privacy-safe public-data ingestion, unique requests, quarantine and Gold reconciliation")
display(gold.orderBy(F.col("requests").desc()).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC **Production limitations:** Mutable NYC 311 records require a refresh window,
# MAGIC watermarks and reconciliation. API paging is not an immutable snapshot.
# MAGIC Multiple date-window runs should be ingested into a common landing zone,
# MAGIC with explicit source manifests and repeatable backfills. This notebook
# MAGIC is intentionally not scheduled against a 23-million-row dataset by default.
