"""Databricks notebook: requires a Spark cluster, widgets and authenticated storage.
Deployment integration example only; not executed in GitHub CI.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("input_uri", "")
dbutils.widgets.text("output_uri", "")
input_uri = dbutils.widgets.get("input_uri")
output_uri = dbutils.widgets.get("output_uri")
if not input_uri or not output_uri:
    raise ValueError("Configure input_uri and output_uri")

raw = spark.read.option("header", True).csv(input_uri)
raw.write.format("delta").mode("overwrite").save(output_uri + "/bronze")
clean = (raw.withColumn("amount", F.col("amount").cast("double"))
    .withColumn("order_date", F.to_date("order_date"))
    .filter(F.col("amount") >= 0).filter(F.col("order_date").isNotNull())
    .filter(F.col("order_id").isNotNull()))
window = Window.partitionBy("order_id").orderBy(F.col("order_date").desc())
clean = clean.withColumn("rn", F.row_number().over(window)).filter("rn=1").drop("rn")
clean.write.format("delta").mode("overwrite").save(output_uri + "/silver")
(clean.filter(F.col("status") == "paid").groupBy("order_date")
    .agg(F.count("order_id").alias("orders"), F.round(F.sum("amount"), 2).alias("revenue"))
    .write.format("delta").mode("overwrite").save(output_uri + "/gold"))
