# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
SILVER_PATH = f"s3://{S3_BUCKET}/delta/silver"
BRONZE_SCHEMA = "grocery_bronze"
SILVER_SCHEMA = "grocery_silver"

products = spark.table(f"{BRONZE_SCHEMA}.bronze_products")
aisles = spark.table(f"{BRONZE_SCHEMA}.bronze_aisles")
departments = spark.table(f"{BRONZE_SCHEMA}.bronze_departments")

dim_aisle = (
    aisles.select(F.expr("try_cast(aisle_id as int)").alias("aisle_id"), F.trim(F.lower(F.col("aisle"))).alias("aisle"))
    .filter(F.col("aisle_id").isNotNull())
    .dropDuplicates(["aisle_id"])
    .withColumn("silver_updated_ts", F.current_timestamp())
)

dim_department = (
    departments.select(F.expr("try_cast(department_id as int)").alias("department_id"), F.trim(F.lower(F.col("department"))).alias("department"))
    .filter(F.col("department_id").isNotNull())
    .dropDuplicates(["department_id"])
    .withColumn("silver_updated_ts", F.current_timestamp())
)

products_clean = products.select(
    F.expr("try_cast(product_id as int)").alias("product_id"),
    F.trim(F.col("product_name")).alias("product_name"),
    F.expr("try_cast(aisle_id as int)").alias("aisle_id"),
    F.expr("try_cast(department_id as int)").alias("department_id"),
    F.col("product_id").alias("raw_product_id"),
    F.col("aisle_id").alias("raw_aisle_id"),
    F.col("department_id").alias("raw_department_id"),
    F.col("source_file"),
    F.col("bronze_ingestion_ts")
)

malformed_products = (
    products_clean.filter(F.col("product_id").isNull() | F.col("aisle_id").isNull() | F.col("department_id").isNull())
    .withColumn("quarantine_reason", F.lit("Invalid numeric key in product source"))
    .withColumn("quarantine_ts", F.current_timestamp())
)

valid_products = products_clean.filter(F.col("product_id").isNotNull() & F.col("aisle_id").isNotNull() & F.col("department_id").isNotNull())

dim_product = (
    valid_products.alias("p")
    .join(dim_aisle.alias("a"), F.col("p.aisle_id") == F.col("a.aisle_id"), "left")
    .join(dim_department.alias("d"), F.col("p.department_id") == F.col("d.department_id"), "left")
    .select("p.product_id", "p.product_name", "p.aisle_id", "a.aisle", "p.department_id", "d.department")
    .dropDuplicates(["product_id"])
    .withColumn("is_current", F.lit(True))
    .withColumn("valid_from", F.current_timestamp())
    .withColumn("valid_to", F.lit(None).cast("timestamp"))
    .withColumn("silver_updated_ts", F.current_timestamp())
)

def write_silver(df, table_name):
    output_path = f"{SILVER_PATH}/{table_name}"
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.{table_name} USING DELTA LOCATION '{output_path}'")
    print(f"{SILVER_SCHEMA}.{table_name}: {df.count():,} rows")

write_silver(dim_aisle, "dim_aisle")
write_silver(dim_department, "dim_department")
write_silver(dim_product, "dim_product")
write_silver(malformed_products, "quarantine_malformed_products")

spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}").show(truncate=False)
print("Silver dimensions completed successfully.")
