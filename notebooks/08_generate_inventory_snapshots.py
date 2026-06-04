# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

S3_BUCKET = "retail-demand-intelligence-project"
SILVER_PATH = f"s3://{S3_BUCKET}/delta/silver"
SILVER_SCHEMA = "grocery_silver"

fact = spark.table(f"{SILVER_SCHEMA}.fact_order_products")

product_demand_by_order = (
    fact.filter(F.col("product_id").isNotNull())
    .groupBy("order_number", "product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type")
    .agg(F.count("*").alias("units_sold"), F.sum("reordered").alias("reordered_units"))
)

product_base = fact.filter(F.col("product_id").isNotNull()).select(
    "product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type"
).dropDuplicates(["product_id"])

snapshot_numbers = fact.select("order_number").dropDuplicates().filter(F.col("order_number").isNotNull())

inventory_base = (
    product_base.crossJoin(snapshot_numbers)
    .join(product_demand_by_order, ["order_number", "product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type"], "left")
    .fillna({"units_sold": 0, "reordered_units": 0})
)

w = Window.partitionBy("product_id").orderBy("order_number").rowsBetween(Window.unboundedPreceding, Window.currentRow)

inventory_snapshots = (
    inventory_base
    .withColumn("initial_inventory_qty", F.lit(500))
    .withColumn("restock_qty", F.when((F.col("order_number") % 10) == 0, F.lit(250)).otherwise(F.lit(0)))
    .withColumn("cumulative_units_sold", F.sum("units_sold").over(w))
    .withColumn("cumulative_restock_qty", F.sum("restock_qty").over(w))
    .withColumn("ending_inventory_qty", F.greatest(F.col("initial_inventory_qty") + F.col("cumulative_restock_qty") - F.col("cumulative_units_sold"), F.lit(0)))
    .withColumn("reorder_point", F.lit(50))
    .withColumn("inventory_snapshot_number", F.col("order_number"))
    .withColumn("silver_updated_ts", F.current_timestamp())
    .select("inventory_snapshot_number", "product_id", "product_name", "aisle", "department", "ai_product_theme", "ai_demand_type", "units_sold", "reordered_units", "initial_inventory_qty", "restock_qty", "cumulative_units_sold", "cumulative_restock_qty", "ending_inventory_qty", "reorder_point", "silver_updated_ts")
)

output_path = f"{SILVER_PATH}/fact_inventory_snapshots"
inventory_snapshots.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.fact_inventory_snapshots USING DELTA LOCATION '{output_path}'")

print(f"inventory snapshot rows: {spark.table(f'{SILVER_SCHEMA}.fact_inventory_snapshots').count():,}")
print("Inventory snapshot generation completed successfully.")
