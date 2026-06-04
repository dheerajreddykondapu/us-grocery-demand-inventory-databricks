-- ============================================================
-- U.S. Grocery Demand & Inventory Intelligence Dashboard
-- ============================================================

-- 01. Total order-product records processed
SELECT COUNT(*) AS total_order_product_records
FROM grocery_silver.fact_order_products;

-- 02. High-risk stockout products
SELECT COUNT(*) AS high_risk_stockout_products
FROM grocery_gold.stockout_risk
WHERE stockout_risk_tier IN ('STOCKOUT', 'CRITICAL', 'HIGH');

-- 03. Total overstock exposure units
SELECT SUM(estimated_capital_exposure_units) AS total_overstock_exposure_units
FROM grocery_gold.overstock_alerts;

-- 04. Top departments by demand
SELECT
  department,
  SUM(units_sold) AS total_units_sold
FROM grocery_gold.daily_product_demand
GROUP BY department
ORDER BY total_units_sold DESC;

-- 05. Top reordered products
SELECT
  product_name,
  department,
  aisle,
  total_units_ordered,
  total_reordered_units,
  ROUND(reorder_rate, 4) AS reorder_rate
FROM grocery_gold.reorder_metrics
WHERE total_units_ordered >= 100
ORDER BY reorder_rate DESC
LIMIT 25;

-- 06. Demand by AI product theme
SELECT
  COALESCE(ai_product_theme, 'unclassified') AS ai_product_theme,
  SUM(total_units_ordered) AS total_units_ordered,
  SUM(total_reordered_units) AS total_reordered_units,
  ROUND(SUM(total_reordered_units) / SUM(total_units_ordered), 4) AS reorder_rate
FROM grocery_gold.reorder_metrics
GROUP BY COALESCE(ai_product_theme, 'unclassified')
ORDER BY total_units_ordered DESC;

-- 07. Demand by AI demand type
SELECT
  COALESCE(ai_demand_type, 'unclassified') AS ai_demand_type,
  SUM(total_units_ordered) AS total_units_ordered,
  SUM(total_reordered_units) AS total_reordered_units,
  ROUND(SUM(total_reordered_units) / SUM(total_units_ordered), 4) AS reorder_rate
FROM grocery_gold.reorder_metrics
GROUP BY COALESCE(ai_demand_type, 'unclassified')
ORDER BY total_units_ordered DESC;

-- 08. Stockout risk by tier
SELECT
  stockout_risk_tier,
  COUNT(*) AS product_count,
  ROUND(AVG(days_of_stock_proxy), 2) AS avg_days_of_stock
FROM grocery_gold.stockout_risk
GROUP BY stockout_risk_tier
ORDER BY product_count DESC;

-- 09. High-priority stockout products
SELECT
  product_name,
  department,
  aisle,
  ai_product_theme,
  ai_demand_type,
  ending_inventory_qty,
  ROUND(avg_units_per_order_window_30, 2) AS avg_units_per_order_window_30,
  ROUND(days_of_stock_proxy, 2) AS days_of_stock_proxy,
  stockout_risk_tier,
  priority_score
FROM grocery_gold.stockout_risk
WHERE stockout_risk_tier IN ('STOCKOUT', 'CRITICAL', 'HIGH')
ORDER BY priority_score DESC, days_of_stock_proxy ASC
LIMIT 50;

-- 10. Overstock exposure by tier
SELECT
  overstock_tier,
  COUNT(*) AS product_count,
  SUM(estimated_capital_exposure_units) AS exposure_units
FROM grocery_gold.overstock_alerts
GROUP BY overstock_tier
ORDER BY exposure_units DESC;

-- 11. Top basket product pairs
SELECT
  product_name_a,
  product_name_b,
  department_a,
  department_b,
  ai_product_theme_a,
  ai_product_theme_b,
  co_purchase_count
FROM grocery_gold.basket_affinity
ORDER BY co_purchase_count DESC
LIMIT 50;
