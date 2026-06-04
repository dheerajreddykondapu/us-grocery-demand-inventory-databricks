# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
GOLD_PATH = f"s3://{S3_BUCKET}/delta/gold"
SILVER_SCHEMA = "grocery_silver"
GOLD_SCHEMA = "grocery_gold"

fact = spark.table(f"{SILVER_SCHEMA}.fact_order_products")

reorder_metrics = (
    fact.filter(F.col("product_id").isNotNull())
    .groupBy("product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type")
    .agg(
        F.count("*").alias("total_units_ordered"),
        F.sum("reordered").alias("total_reordered_units"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.avg("add_to_cart_order").alias("avg_cart_position")
    )
    .withColumn("reorder_rate", F.when(F.col("total_units_ordered") > 0, F.col("total_reordered_units") / F.col("total_units_ordered")).otherwise(F.lit(0)))
    .withColumn("gold_updated_ts", F.current_timestamp())
)

output_path = f"{GOLD_PATH}/reorder_metrics"
reorder_metrics.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.reorder_metrics USING DELTA LOCATION '{output_path}'")

print(f"reorder_metrics rows: {spark.table(f'{GOLD_SCHEMA}.reorder_metrics').count():,}")
print("Gold reorder metrics completed successfully.")
