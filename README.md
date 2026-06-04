# U.S. Grocery Demand & Inventory Intelligence Pipeline

End-to-end AWS + Databricks lakehouse project for U.S. grocery demand, reorder behavior, inventory risk, overstock exposure, basket affinity, and AI-enriched product segmentation using the Instacart Market Basket dataset.

## Architecture

```text
Manual Trigger
   ↓
AWS Lambda
   ↓
Amazon S3 Raw Zone
   ↓
Databricks Job / Workflow
   ↓
Bronze Delta Tables
   ↓
Silver Cleaned + AI-Enriched Tables
   ↓
Gold Business KPI Tables
   ↓
Databricks SQL Dashboard
```

## Project Highlights

- Ingested real Instacart grocery dataset from Kaggle into Amazon S3 using AWS Lambda.
- Processed 3.4M+ orders and 33.8M+ order-product records using Databricks and PySpark.
- Built Bronze, Silver, and Gold Delta Lake layers following the medallion architecture.
- Added Silver-layer data quality handling with malformed product quarantine logic.
- Used Databricks AI Functions on a product sample and rule-based fallback logic for full product coverage.
- Created Gold tables for demand metrics, reorder behavior, stockout risk, overstock alerts, and basket affinity.
- Built dashboard-ready datasets for Databricks SQL reporting.
- Designed orchestration through Databricks Jobs with manual run mode to avoid recurring cloud costs.

## Tech Stack

```text
AWS Lambda, Amazon S3, AWS IAM, Databricks, PySpark, Spark SQL, Delta Lake, Unity Catalog, Databricks Jobs, Databricks AI Functions, Databricks SQL
```

## S3 Bucket

```text
retail-demand-intelligence-project
```

## S3 Layout

```text
s3://retail-demand-intelligence-project/raw/instacart/orders/
s3://retail-demand-intelligence-project/raw/instacart/order_products_prior/
s3://retail-demand-intelligence-project/raw/instacart/order_products_train/
s3://retail-demand-intelligence-project/raw/instacart/products/
s3://retail-demand-intelligence-project/raw/instacart/aisles/
s3://retail-demand-intelligence-project/raw/instacart/departments/

s3://retail-demand-intelligence-project/delta/bronze/
s3://retail-demand-intelligence-project/delta/silver/
s3://retail-demand-intelligence-project/delta/gold/
```

## Pipeline Layers

### Bronze
Raw CSV data from S3 is converted into Delta tables with source file and ingestion timestamp metadata.

### Silver
Data is cleaned, validated, modeled, and enriched. This layer includes product, aisle, department dimensions, AI-enriched product dimension, fact order-products table, and derived inventory snapshots.

### Gold
Business-ready KPI tables for demand trends, reorder metrics, stockout risk, overstock exposure, and basket affinity.

## Final Tables

### Bronze
```text
grocery_bronze.bronze_orders
grocery_bronze.bronze_order_products_prior
grocery_bronze.bronze_order_products_train
grocery_bronze.bronze_products
grocery_bronze.bronze_aisles
grocery_bronze.bronze_departments
```

### Silver
```text
grocery_silver.dim_aisle
grocery_silver.dim_department
grocery_silver.dim_product
grocery_silver.dim_product_ai
grocery_silver.fact_order_products
grocery_silver.fact_inventory_snapshots
grocery_silver.quarantine_malformed_products
```

### Gold
```text
grocery_gold.daily_product_demand
grocery_gold.reorder_metrics
grocery_gold.stockout_risk
grocery_gold.overstock_alerts
grocery_gold.basket_affinity
```

## Data Volumes

```text
orders: 3,421,083 rows
order_products_prior: 32,434,489 rows
order_products_train: 1,384,617 rows
products: 49,688 rows
aisles: 134 rows
departments: 21 rows
fact_order_products: 33,819,106 rows
dim_product_ai: 49,687 rows
fact_inventory_snapshots: 4,968,500 rows
daily_product_demand: 1,808,618 rows
```

## Databricks AI Enrichment

The project uses Databricks AI Functions to classify a representative product sample into product themes and demand types. To control runtime and cost, a hybrid strategy is used:

```text
Databricks AI Function: 1,000 products
Rule-based fallback: 48,687 products
```

This creates a full AI-enriched product dimension while keeping the project practical for portfolio execution.

## Pipeline Orchestration

This project is implemented as a manually triggered end-to-end ELT pipeline to avoid unnecessary cloud costs during portfolio development.

- AWS Lambda is run on demand to ingest raw data into S3.
- Databricks Jobs are run on demand to execute notebook tasks in dependency order.
- The design can be scheduled using AWS EventBridge and Databricks Job schedules in a production environment.

## Run Order

```text
00_setup
01_validate_s3_raw_files
02_bronze_instacart_ingestion
03_silver_dimensions
04_silver_product_ai_classification
05_silver_fact_order_products
06_gold_demand_metrics
07_gold_reorder_metrics
08_generate_inventory_snapshots
09_gold_stockout_risk
10_gold_overstock_alerts
11_gold_basket_affinity
12_final_validation
```

## Dashboard Outputs

The Databricks SQL dashboard includes:

- Total order-product records processed
- Top departments by demand
- Demand by AI product theme
- Demand by AI demand type
- Top reordered products
- Stockout risk by tier
- Overstock exposure by tier
- Top basket product pairs

## Resume Bullet

Built an on-demand AWS and Databricks ELT pipeline using Lambda, S3, Databricks Jobs, PySpark, Delta Lake, and Databricks AI Functions to process 33M+ Instacart order-product records across Bronze, Silver, and Gold layers for demand trends, reorder behavior, inventory risk, overstock exposure, basket affinity, and AI-enriched product segmentation.
