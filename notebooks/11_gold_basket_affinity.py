# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
GOLD_PATH = f"s3://{S3_BUCKET}/delta/gold"
SILVER_SCHEMA = "grocery_silver"
GOLD_SCHEMA = "grocery_gold"

fact = spark.table(f"{SILVER_SCHEMA}.fact_order_products")

basket_base = (
    fact.filter(F.col("product_id").isNotNull())
    .select("order_id", "product_id", "product_name", "department", "aisle", "ai_product_theme", "ai_demand_type")
    .dropDuplicates(["order_id", "product_id"])
)

a = basket_base.select(
    F.col("order_id"), F.col("product_id").alias("product_id_a"), F.col("product_name").alias("product_name_a"),
    F.col("department").alias("department_a"), F.col("aisle").alias("aisle_a"),
    F.col("ai_product_theme").alias("ai_product_theme_a"), F.col("ai_demand_type").alias("ai_demand_type_a")
)

b = basket_base.select(
    F.col("order_id"), F.col("product_id").alias("product_id_b"), F.col("product_name").alias("product_name_b"),
    F.col("department").alias("department_b"), F.col("aisle").alias("aisle_b"),
    F.col("ai_product_theme").alias("ai_product_theme_b"), F.col("ai_demand_type").alias("ai_demand_type_b")
)

basket_affinity = (
    a.join(b, "order_id", "inner")
    .filter(F.col("product_id_a") < F.col("product_id_b"))
    .groupBy("product_id_a", "product_name_a", "department_a", "aisle_a", "ai_product_theme_a", "ai_demand_type_a", "product_id_b", "product_name_b", "department_b", "aisle_b", "ai_product_theme_b", "ai_demand_type_b")
    .agg(F.countDistinct("order_id").alias("co_purchase_count"))
    .filter(F.col("co_purchase_count") >= 10)
    .withColumn("gold_updated_ts", F.current_timestamp())
)

output_path = f"{GOLD_PATH}/basket_affinity"
basket_affinity.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.basket_affinity USING DELTA LOCATION '{output_path}'")

print(f"basket_affinity rows: {spark.table(f'{GOLD_SCHEMA}.basket_affinity').count():,}")
print("Gold basket affinity completed successfully.")
