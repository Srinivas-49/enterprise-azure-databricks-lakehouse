-- MANUAL post-run governance step; requires APPLY TAG and owner or equivalent access.
-- These are ordinary metadata labels, not governed account-level policies.
-- Run after 01_end_to_end_lakehouse and 03_multisource_spark_gold.

ALTER TABLE portfolio_multisource.bronze_orders
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'bronze');
ALTER TABLE portfolio_multisource.bronze_customers
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'bronze');
ALTER TABLE portfolio_multisource.bronze_payments
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'bronze');
ALTER TABLE portfolio_multisource.silver_reconciled_orders
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'silver');
ALTER TABLE portfolio_multisource.gold_daily_revenue
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'gold');
ALTER TABLE portfolio_multisource.gold_customer_segments
  SET TAGS ('data_classification' = 'synthetic', 'data_layer' = 'gold');
