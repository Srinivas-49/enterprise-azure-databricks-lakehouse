-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exploratory SQL | Enterprise Lakehouse
-- MAGIC Execute 01_end_to_end_lakehouse first. This notebook uses only synthetic data.

-- COMMAND ----------
USE SCHEMA portfolio_lakehouse;

-- COMMAND ----------
SELECT status, order_count FROM gold_order_status ORDER BY order_count DESC;

-- COMMAND ----------
SELECT order_date, paid_orders, refunded_orders, paid_revenue
FROM gold_daily_revenue ORDER BY order_date;

-- COMMAND ----------
SELECT customer_id, total_orders, paid_orders, lifetime_paid_revenue
FROM gold_customer_360
ORDER BY lifetime_paid_revenue DESC LIMIT 20;

-- COMMAND ----------
SELECT event_id, rejection_reason FROM silver_quarantine ORDER BY event_id;

-- COMMAND ----------
SELECT source_event_id, order_id, status, updated_at
FROM silver_order_event_history
WHERE order_id IN ('ord-0000000','ord-0000005')
ORDER BY order_id, updated_at;

-- COMMAND ----------
DESCRIBE HISTORY silver_orders;

-- COMMAND ----------
SELECT * FROM ops_pipeline_audit;
