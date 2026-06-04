# Data Dictionary

## Bronze Tables

- `bronze_orders`: raw Instacart orders.
- `bronze_order_products_prior`: raw prior order-product records.
- `bronze_order_products_train`: raw train order-product records.
- `bronze_products`: raw product metadata.
- `bronze_aisles`: raw aisle lookup.
- `bronze_departments`: raw department lookup.

## Silver Tables

- `dim_product`: cleaned product dimension.
- `dim_product_ai`: AI-enriched product dimension with product theme and demand type.
- `dim_aisle`: cleaned aisle dimension.
- `dim_department`: cleaned department dimension.
- `fact_order_products`: joined order-product fact table with product enrichment.
- `fact_inventory_snapshots`: derived inventory snapshots from demand behavior.
- `quarantine_malformed_products`: invalid product records captured for audit.

## Gold Tables

- `daily_product_demand`: demand and rolling reorder metrics by product/order number.
- `reorder_metrics`: product-level reorder KPIs.
- `stockout_risk`: stockout risk scoring by product.
- `overstock_alerts`: overstock exposure by product.
- `basket_affinity`: product pairs commonly purchased together.
