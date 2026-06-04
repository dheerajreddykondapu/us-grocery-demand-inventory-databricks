# Databricks notebook source
from pyspark.sql import functions as F

S3_BUCKET = "retail-demand-intelligence-project"
SILVER_PATH = f"s3://{S3_BUCKET}/delta/silver"
SILVER_SCHEMA = "grocery_silver"
AI_SAMPLE_SIZE = 1000

dim_product = spark.table(f"{SILVER_SCHEMA}.dim_product")

products_for_ai = (
    dim_product.select("product_id", "product_name", "aisle", "department")
    .dropDuplicates(["product_id"])
    .filter(F.col("product_name").isNotNull())
    .orderBy("product_id")
    .limit(AI_SAMPLE_SIZE)
)
products_for_ai.createOrReplaceTempView("products_for_ai")

product_ai_sample = spark.sql("""
SELECT
    product_id,
    product_name,
    aisle,
    department,
    ai_classify(
        product_name,
        array('fresh_produce','dairy_and_eggs','meat_and_seafood','frozen_food','beverages','snacks','bakery','pantry_staples','health_and_wellness','household','baby_and_kids','pet_supplies','personal_care','other')
    ) AS ai_product_theme,
    ai_classify(
        product_name,
        array('essential','impulse','premium','seasonal','convenience','health_focused','budget_friendly','other')
    ) AS ai_demand_type,
    'databricks_ai_function' AS classification_method,
    current_timestamp() AS ai_classified_ts
FROM products_for_ai
""")

product_fallback_full = (
    dim_product
    .withColumn(
        "fallback_product_theme",
        F.when(F.lower(F.col("department")).contains("produce"), "fresh_produce")
         .when(F.lower(F.col("department")).contains("dairy"), "dairy_and_eggs")
         .when(F.lower(F.col("department")).contains("meat"), "meat_and_seafood")
         .when(F.lower(F.col("department")).contains("frozen"), "frozen_food")
         .when(F.lower(F.col("department")).contains("beverages"), "beverages")
         .when(F.lower(F.col("department")).contains("snacks"), "snacks")
         .when(F.lower(F.col("department")).contains("bakery"), "bakery")
         .when(F.lower(F.col("department")).contains("household"), "household")
         .when(F.lower(F.col("department")).contains("babies"), "baby_and_kids")
         .when(F.lower(F.col("department")).contains("pets"), "pet_supplies")
         .when(F.lower(F.col("department")).contains("personal care"), "personal_care")
         .otherwise("other")
    )
    .withColumn(
        "fallback_demand_type",
        F.when(F.lower(F.col("product_name")).rlike("organic|protein|gluten|keto|vegan|plant|non-gmo"), "health_focused")
         .when(F.lower(F.col("product_name")).rlike("premium|reserve|artisan|specialty|gourmet"), "premium")
         .when(F.lower(F.col("product_name")).rlike("snack|chips|cookie|candy|chocolate|popcorn"), "impulse")
         .when(F.lower(F.col("product_name")).rlike("family|value|bulk|large|party"), "budget_friendly")
         .when(F.lower(F.col("product_name")).rlike("frozen|ready|instant|microwave|quick"), "convenience")
         .otherwise("essential")
    )
)

product_ai_final = (
    product_fallback_full.alias("p")
    .join(
        product_ai_sample.select(
            "product_id",
            F.col("ai_product_theme").alias("sample_ai_product_theme"),
            F.col("ai_demand_type").alias("sample_ai_demand_type"),
            "classification_method",
            "ai_classified_ts"
        ).alias("ai"),
        "product_id",
        "left"
    )
    .select(
        F.col("p.product_id"), F.col("p.product_name"), F.col("p.aisle_id"), F.col("p.aisle"),
        F.col("p.department_id"), F.col("p.department"),
        F.coalesce(F.col("sample_ai_product_theme"), F.col("fallback_product_theme")).alias("ai_product_theme"),
        F.coalesce(F.col("sample_ai_demand_type"), F.col("fallback_demand_type")).alias("ai_demand_type"),
        F.coalesce(F.col("classification_method"), F.lit("rule_based_fallback")).alias("classification_method"),
        F.coalesce(F.col("ai_classified_ts"), F.current_timestamp()).alias("ai_classified_ts"),
        F.col("p.is_current"), F.col("p.valid_from"), F.col("p.valid_to"), F.col("p.silver_updated_ts")
    )
)

output_path = f"{SILVER_PATH}/dim_product_ai"
product_ai_final.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(output_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS {SILVER_SCHEMA}.dim_product_ai USING DELTA LOCATION '{output_path}'")

print(f"Final AI-enriched product rows: {product_ai_final.count():,}")
display(spark.table(f"{SILVER_SCHEMA}.dim_product_ai").groupBy("classification_method").count())
print("Product AI classification completed.")
