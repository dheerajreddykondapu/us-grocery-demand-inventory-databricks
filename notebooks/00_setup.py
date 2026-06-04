# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

PROJECT_NAME = "us_grocery_demand_inventory_intelligence"
S3_BUCKET = "retail-demand-intelligence-project"

RAW_BASE_PATH = f"s3://{S3_BUCKET}/raw"
INSTACART_RAW_PATH = f"{RAW_BASE_PATH}/instacart"
AMAZON_REVIEWS_RAW_PATH = f"{RAW_BASE_PATH}/amazon_reviews"

DELTA_BASE_PATH = f"s3://{S3_BUCKET}/delta"
BRONZE_PATH = f"{DELTA_BASE_PATH}/bronze"
SILVER_PATH = f"{DELTA_BASE_PATH}/silver"
GOLD_PATH = f"{DELTA_BASE_PATH}/gold"

BRONZE_SCHEMA = "grocery_bronze"
SILVER_SCHEMA = "grocery_silver"
GOLD_SCHEMA = "grocery_gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_SCHEMA}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_SCHEMA}")

print("Schemas created / validated successfully.")
print(f"Project name: {PROJECT_NAME}")
print(f"S3 bucket: {S3_BUCKET}")
print(f"Raw path: {RAW_BASE_PATH}")
print(f"Bronze path: {BRONZE_PATH}")
print(f"Silver path: {SILVER_PATH}")
print(f"Gold path: {GOLD_PATH}")

spark.sql("SHOW DATABASES").show(truncate=False)
