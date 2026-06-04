-- Optional Delta optimization commands
OPTIMIZE grocery_gold.daily_product_demand;
OPTIMIZE grocery_gold.reorder_metrics;
OPTIMIZE grocery_gold.stockout_risk;
OPTIMIZE grocery_gold.overstock_alerts;
OPTIMIZE grocery_gold.basket_affinity;

OPTIMIZE grocery_gold.daily_product_demand ZORDER BY (product_id, department);
OPTIMIZE grocery_gold.reorder_metrics ZORDER BY (product_id, department);
