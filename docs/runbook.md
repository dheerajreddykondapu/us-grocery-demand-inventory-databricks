# Runbook

## Manual Run Order

1. Run AWS Lambda `instacart-raw-ingestion-lambda` manually.
2. Validate files landed in S3 under `raw/instacart/.../ingestion_date=YYYY-MM-DD/`.
3. Run Databricks Job `grocery-demand-inventory-pipeline` using **Run now**.
4. Validate Bronze, Silver, and Gold table row counts.
5. Refresh Databricks SQL dashboard.

## Databricks Notebook Order

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

## Scheduling Note

Schedules are intentionally disabled to avoid recurring cloud costs. The project can be scheduled using EventBridge and Databricks Job schedules in a production environment.
