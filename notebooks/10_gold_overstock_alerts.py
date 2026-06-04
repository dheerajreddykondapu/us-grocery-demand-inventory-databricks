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
    F.avg("avg_units_per_order_window_30").alias("avg_units_per_order_window_30"), F.avg("reorder_rate").alias("avg_reorder_rate")
)

overstock_alerts = (
    latest_inventory.alias("i")
    .join(latest_demand.alias("d"), "product_id", "left")
    .withColumn("days_of_stock_proxy", F.when(F.col("avg_units_per_order_window_30") > 0, F.col("ending_inventory_qty") / F.col("avg_units_per_order_window_30")).otherwise(F.lit(999)))
    .withColumn("overstock_tier", F.when(F.col("days_of_stock_proxy") >= 120, "SEVERE_OVERSTOCK").when(F.col("days_of_stock_proxy") >= 90, "HIGH_OVERSTOCK").when(F.col("days_of_stock_proxy") >= 60, "MODERATE_OVERSTOCK").otherwise("NORMAL"))
    .withColumn("estimated_capital_exposure_units", F.col("ending_inventory_qty"))
    .withColumn("gold_updated_ts", F.current_timestamp())
    .filter(F.col("overstock_tier") != "NORMAL")
    .select(F.col("i.inventory_snapshot_number"), F.col("i.product_id"), F.coalesce(F.col("d.product_name"), F.col("i.product_name")).alias("product_name"), F.coalesce(F.col("d.aisle"), F.col("i.aisle")).alias("aisle"), F.coalesce(F.col("d.department"), F.col("i.department")).alias("department"), F.coalesce(F.col("d.ai_product_theme"), F.col("i.ai_product_theme")).alias("ai_product_theme"), F.coalesce(F.col("d.ai_demand_type"), F.col("i.ai_demand_type")).alias("ai_demand_type"), F.col("i.ending_inventory_qty"), F.col("d.avg_units_per_order_window_30"), F.col("d.avg_reorder_rate"), F.col("days_of_stock_proxy"), F.col("overstock_tier"), F.col("estimated_capital_exposure_units"), F.col("gold_updated_ts"))
)

output_path = f"{GOLD_PATH}/overstock_alerts"
overstock_alerts.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.overstock_alerts USING DELTA LOCATION '{output_path}'")

print(f"overstock_alerts rows: {spark.table(f'{GOLD_SCHEMA}.overstock_alerts').count():,}")
print("Gold overstock alerts completed successfully.")
