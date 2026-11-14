"""Databricks optimization example, not deployed or benchmarked."""
from azure.optimization_policy import choose_join_strategy, choose_partition_column

def build_gold(spark, silver_path, dimension_path, gold_path, dimension_bytes, zorder=False):
    from pyspark.sql import functions as F
    from pyspark.sql.functions import broadcast
    facts = spark.read.format("delta").load(silver_path)
    dimension = spark.read.format("delta").load(dimension_path)
    if choose_join_strategy(dimension_bytes) == "broadcast":
        dimension = broadcast(dimension)
    joined = facts.join(dimension, "customer_id", "left").withColumn(
        "order_month", F.date_format("order_date", "yyyy-MM"))
    joined.write.format("delta").mode("overwrite").partitionBy(
        choose_partition_column(set(joined.columns))).save(gold_path)
    if zorder:
        spark.sql("OPTIMIZE delta.\x60" + gold_path.replace("\x60", "") + "\x60 ZORDER BY (customer_id)")
