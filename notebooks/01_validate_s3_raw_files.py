# Databricks notebook source
S3_BUCKET = "retail-demand-intelligence-project"
INSTACART_RAW_PATH = f"s3://{S3_BUCKET}/raw/instacart"

raw_folders = {
    "orders": f"{INSTACART_RAW_PATH}/orders/",
    "order_products_prior": f"{INSTACART_RAW_PATH}/order_products_prior/",
    "order_products_train": f"{INSTACART_RAW_PATH}/order_products_train/",
    "products": f"{INSTACART_RAW_PATH}/products/",
    "aisles": f"{INSTACART_RAW_PATH}/aisles/",
    "departments": f"{INSTACART_RAW_PATH}/departments/"
}

for name, path in raw_folders.items():
    print(f"\nChecking {name}: {path}")
    files = dbutils.fs.ls(path)
    for f in files:
        print(f"  {f.path}")

csv_paths = {k: f"{v}*/*.csv" for k, v in raw_folders.items()}

for name, path in csv_paths.items():
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    print(f"{name}: {df.count():,} rows")
    display(df.limit(5))
