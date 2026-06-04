# Databricks notebook source
schemas = {
    "Bronze": "grocery_bronze",
    "Silver": "grocery_silver",
    "Gold": "grocery_gold"
}

for layer, schema in schemas.items():
    print(f"\n{layer} tables")
    print("-" * 80)
    spark.sql(f"SHOW TABLES IN {schema}").show(truncate=False)

tables = [
    "grocery_bronze.bronze_orders",
    "grocery_bronze.bronze_order_products_prior",
    "grocery_bronze.bronze_order_products_train",
    "grocery_bronze.bronze_products",
    "grocery_bronze.bronze_aisles",
    "grocery_bronze.bronze_departments",
    "grocery_silver.dim_product",
    "grocery_silver.dim_product_ai",
    "grocery_silver.dim_aisle",
    "grocery_silver.dim_department",
    "grocery_silver.fact_order_products",
    "grocery_silver.fact_inventory_snapshots",
    "grocery_silver.quarantine_malformed_products",
    "grocery_gold.daily_product_demand",
    "grocery_gold.reorder_metrics",
    "grocery_gold.stockout_risk",
    "grocery_gold.overstock_alerts",
    "grocery_gold.basket_affinity"
]

for table in tables:
    try:
        count = spark.table(table).count()
        print(f"{table}: {count:,}")
    except Exception as e:
        print(f"{table}: ERROR - {str(e)}")

print("Final validation completed.")
