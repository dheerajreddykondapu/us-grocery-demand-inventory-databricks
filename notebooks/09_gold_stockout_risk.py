# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
GOLD_PATH = f"s3://{S3_BUCKET}/delta/gold"
SILVER_SCHEMA = "grocery_silver"
GOLD_SCHEMA = "grocery_gold"

inventory = spark.table(f"{SILVER_SCHEMA}.fact_inventory_snapshots")
demand = spark.table(f"{GOLD_SCHEMA}.daily_product_demand")

latest_snapshot = inventory.agg(F.max("inventory_snapshot_number")).collect()[0][0]
latest_inventory = inventory.filter(F.col("inventory_snapshot_number") == latest_snapshot)

latest_demand = demand.groupBy("product_id").agg(
    F.max("product_name").alias("product_name"), F.max("aisle").alias("aisle"), F.max("department").alias("department"),
    F.max("ai_product_theme").alias("ai_product_theme"), F.max("ai_demand_type").alias("ai_demand_type"),
    F.avg("avg_units_per_order_window_30").alias("avg_units_per_order_window_30"),
    F.avg("reorder_rate").alias("avg_reorder_rate")
)

stockout_risk = (
    latest_inventory.alias("i")
    .join(latest_demand.alias("d"), "product_id", "left")
    .withColumn("days_of_stock_proxy", F.when(F.col("avg_units_per_order_window_30") > 0, F.col("ending_inventory_qty") / F.col("avg_units_per_order_window_30")).otherwise(F.lit(None)))
    .withColumn("stockout_risk_tier", F.when((F.col("ending_inventory_qty") == 0) & (F.col("avg_units_per_order_window_30") > 0), "STOCKOUT").when(F.col("days_of_stock_proxy") <= 3, "CRITICAL").when(F.col("days_of_stock_proxy") <= 7, "HIGH").when(F.col("days_of_stock_proxy") <= 14, "MEDIUM").otherwise("OK"))
    .withColumn("priority_score", F.when(F.col("stockout_risk_tier") == "STOCKOUT", 100).when(F.col("stockout_risk_tier") == "CRITICAL", 80).when(F.col("stockout_risk_tier") == "HIGH", 60).when(F.col("stockout_risk_tier") == "MEDIUM", 40).otherwise(10))
    .withColumn("gold_updated_ts", F.current_timestamp())
    .select(F.col("i.inventory_snapshot_number"), F.col("i.product_id"), F.coalesce(F.col("d.product_name"), F.col("i.product_name")).alias("product_name"), F.coalesce(F.col("d.aisle"), F.col("i.aisle")).alias("aisle"), F.coalesce(F.col("d.department"), F.col("i.department")).alias("department"), F.coalesce(F.col("d.ai_product_theme"), F.col("i.ai_product_theme")).alias("ai_product_theme"), F.coalesce(F.col("d.ai_demand_type"), F.col("i.ai_demand_type")).alias("ai_demand_type"), F.col("i.ending_inventory_qty"), F.col("i.reorder_point"), F.col("d.avg_units_per_order_window_30"), F.col("d.avg_reorder_rate"), F.col("days_of_stock_proxy"), F.col("stockout_risk_tier"), F.col("priority_score"), F.col("gold_updated_ts"))
)

output_path = f"{GOLD_PATH}/stockout_risk"
stockout_risk.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.stockout_risk USING DELTA LOCATION '{output_path}'")

print(f"stockout_risk rows: {spark.table(f'{GOLD_SCHEMA}.stockout_risk').count():,}")
print("Gold stockout risk completed successfully.")
