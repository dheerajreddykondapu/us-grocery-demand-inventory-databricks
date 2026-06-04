# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
SILVER_PATH = f"s3://{S3_BUCKET}/delta/silver"
BRONZE_SCHEMA = "grocery_bronze"
SILVER_SCHEMA = "grocery_silver"

orders = spark.table(f"{BRONZE_SCHEMA}.bronze_orders")
order_products_prior = spark.table(f"{BRONZE_SCHEMA}.bronze_order_products_prior")
order_products_train = spark.table(f"{BRONZE_SCHEMA}.bronze_order_products_train")
dim_product_ai = spark.table(f"{SILVER_SCHEMA}.dim_product_ai")

order_products_all = order_products_prior.withColumn("eval_set_source", F.lit("prior")).unionByName(
    order_products_train.withColumn("eval_set_source", F.lit("train")), allowMissingColumns=True
)

fact_order_products = (
    order_products_all.alias("op")
    .join(orders.alias("o"), "order_id", "inner")
    .join(dim_product_ai.alias("p"), F.col("op.product_id") == F.col("p.product_id"), "left")
    .select(
        F.col("op.order_id").cast("int").alias("order_id"),
        F.col("o.user_id").cast("int").alias("user_id"),
        F.col("op.product_id").cast("int").alias("product_id"),
        F.col("p.product_name"), F.col("p.aisle_id"), F.col("p.aisle"), F.col("p.department_id"), F.col("p.department"),
        F.col("p.ai_product_theme"), F.col("p.ai_demand_type"), F.col("p.classification_method"),
        F.col("op.add_to_cart_order").cast("int").alias("add_to_cart_order"),
        F.col("op.reordered").cast("int").alias("reordered"),
        F.col("o.eval_set"), F.col("op.eval_set_source"),
        F.col("o.order_number").cast("int").alias("order_number"),
        F.col("o.order_dow").cast("int").alias("order_dow"),
        F.col("o.order_hour_of_day").cast("int").alias("order_hour_of_day"),
        F.col("o.days_since_prior_order").cast("double").alias("days_since_prior_order")
    )
    .withColumn("silver_updated_ts", F.current_timestamp())
    .dropDuplicates(["order_id", "product_id"])
)

output_path = f"{SILVER_PATH}/fact_order_products"
fact_order_products.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.fact_order_products USING DELTA LOCATION '{output_path}'")

print(f"fact_order_products rows: {spark.table(f'{SILVER_SCHEMA}.fact_order_products').count():,}")
display(spark.table(f"{SILVER_SCHEMA}.fact_order_products").groupBy("classification_method").count())
print("Silver fact_order_products completed successfully.")
