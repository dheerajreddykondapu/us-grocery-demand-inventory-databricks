# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
RAW_PATH = f"s3://{S3_BUCKET}/raw/instacart"
BRONZE_PATH = f"s3://{S3_BUCKET}/delta/bronze"
BRONZE_SCHEMA = "grocery_bronze"

def read_csv(path):
    return (
        spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("bronze_ingestion_ts", F.current_timestamp())
    )

def write_bronze(df, table_name):
    output_path = f"{BRONZE_PATH}/{table_name}"
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {BRONZE_SCHEMA}.{table_name} USING DELTA LOCATION '{output_path}'")
    print(f"{BRONZE_SCHEMA}.{table_name}: {df.count():,} rows")

orders_df = read_csv(f"{RAW_PATH}/orders/*/*.csv")
order_products_prior_df = read_csv(f"{RAW_PATH}/order_products_prior/*/*.csv")
order_products_train_df = read_csv(f"{RAW_PATH}/order_products_train/*/*.csv")
products_df = read_csv(f"{RAW_PATH}/products/*/*.csv")
aisles_df = read_csv(f"{RAW_PATH}/aisles/*/*.csv")
departments_df = read_csv(f"{RAW_PATH}/departments/*/*.csv")

write_bronze(orders_df, "bronze_orders")
write_bronze(order_products_prior_df, "bronze_order_products_prior")
write_bronze(order_products_train_df, "bronze_order_products_train")
write_bronze(products_df, "bronze_products")
write_bronze(aisles_df, "bronze_aisles")
write_bronze(departments_df, "bronze_departments")

spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}").show(truncate=False)
print("Bronze ingestion completed successfully.")
