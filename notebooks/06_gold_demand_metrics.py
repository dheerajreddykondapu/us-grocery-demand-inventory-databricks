# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

S3_BUCKET = "retail-demand-intelligence-project"
GOLD_PATH = f"s3://{S3_BUCKET}/delta/gold"
SILVER_SCHEMA = "grocery_silver"
GOLD_SCHEMA = "grocery_gold"

fact = spark.table(f"{SILVER_SCHEMA}.fact_order_products")

product_demand = (
    fact.filter(F.col("product_id").isNotNull())
    .groupBy("order_number", "product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type")
    .agg(
        F.count("*").alias("units_sold"),
        F.sum("reordered").alias("reordered_units"),
        F.countDistinct("user_id").alias("unique_customers"),
        F.avg("add_to_cart_order").alias("avg_cart_position")
    )
)

w_7 = Window.partitionBy("product_id").orderBy("order_number").rowsBetween(-7, 0)
w_30 = Window.partitionBy("product_id").orderBy("order_number").rowsBetween(-30, 0)

gold_demand = (
    product_demand
    .withColumn("units_sold_7_orders", F.sum("units_sold").over(w_7))
    .withColumn("units_sold_30_orders", F.sum("units_sold").over(w_30))
    .withColumn("avg_units_per_order_window_30", F.col("units_sold_30_orders") / F.lit(30))
    .withColumn("reorder_rate", F.when(F.col("units_sold") > 0, F.col("reordered_units") / F.col("units_sold")).otherwise(F.lit(0)))
    .withColumn("gold_updated_ts", F.current_timestamp())
)

output_path = f"{GOLD_PATH}/daily_product_demand"
gold_demand.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.daily_product_demand USING DELTA LOCATION '{output_path}'")

print(f"daily_product_demand rows: {spark.table(f'{GOLD_SCHEMA}.daily_product_demand').count():,}")
print("Gold demand metrics completed successfully.")
