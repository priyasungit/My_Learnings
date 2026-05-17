-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Star Schema model (Central Fact and joining with Dimensions) - Analytical Usecases

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](analytics.png)

-- COMMAND ----------

SELECT * FROM telecom_silver.cdr_silver_tbl

-- COMMAND ----------

--Revenue and Traffic Analysis:  by Region and Device Brand
--Identifies which geographical regions are generating the most revenue and which mobile brands are most used in those areas.
SELECT 
    t.region,
    d.brand,
    COUNT(f.call_id) AS total_calls,
    SUM(f.cost) AS total_revenue,
    ROUND(AVG(f.duration), 2) AS avg_duration_seconds
FROM telecom_silver.cdr_silver_tbl f
LEFT JOIN telecom_gold.dim_towers1_gold_scd1 t ON f.tower_id = t.tower_id
LEFT JOIN telecom_gold.dim_device_gold_scd1 d ON f.device_id = d.device_id
WHERE f.call_status = 'SUCCESS'
GROUP BY t.region, d.brand
ORDER BY total_revenue DESC;

-- COMMAND ----------

--Network Performance Metrics: 5G vs 4G by Device Type
--Compare how different device categories (Smartphones vs Tablets) are performing across 5G and 4G towers in terms of call volume and revenue
SELECT 
    t.network_type,
    d.device_type,
    COUNT(f.call_id) AS call_count,
    SUM(f.duration) / 60 AS total_minutes,
    SUM(f.cost) AS total_revenue
FROM telecom_silver.cdr_silver_tbl f
LEFT JOIN telecom_gold.dim_towers1_gold_scd1 t ON f.tower_id = t.tower_id
LEFT JOIN telecom_gold.dim_device_gold_scd1 d ON f.device_id = d.device_id
GROUP BY t.network_type, d.device_type
ORDER BY t.network_type, total_revenue DESC;

-- COMMAND ----------

--Regional Call Quality & Drop Analysis: Identify "Hotspots" with high failure rates, segmented by the Operating System (OS) to see if specific software versions are struggling on certain towers
SELECT 
    t.city,
    t.tower_name,
    d.os,
    COUNT(CASE WHEN f.call_status = 'FAILED' THEN 1 END) AS failed_calls,
    COUNT(f.call_id) AS total_calls,
    ROUND((COUNT(CASE WHEN f.call_status = 'FAILED' THEN 1 END) / COUNT(f.call_id)) * 100, 2) AS failure_rate_pct
FROM telecom_silver.cdr_silver_tbl f
LEFT JOIN telecom_gold.dim_towers1_gold_scd1 t ON f.tower_id = t.tower_id
LEFT JOIN telecom_gold.dim_device_gold_scd1 d ON f.device_id = d.device_id
GROUP BY t.city, t.tower_name, d.os
HAVING total_calls > 10
ORDER BY failure_rate_pct DESC;

-- COMMAND ----------

--High-Value Customer Profiling : Identify the top-spending customers and see their preferred device models and the typical network environment (Region/Type) they operate in.
SELECT 
    f.caller_id,
    d.brand,
    d.model,
    t.region,
    t.network_type,
    SUM(f.cost) AS total_spend,
    COUNT(f.call_id) AS frequency
FROM telecom_silver.cdr_silver_tbl f
LEFT JOIN telecom_gold.dim_towers1_gold_scd1 t ON f.tower_id = t.tower_id
LEFT JOIN telecom_gold.dim_device_gold_scd1 d ON f.device_id = d.device_id
GROUP BY f.caller_id, d.brand, d.model, t.region, t.network_type
ORDER BY total_spend DESC
LIMIT 20;

-- COMMAND ----------

--Incremental Gold table Enrichment: Instead of re-processing the entire Fact table to join with Dimensions, to incrementally join the newly inserted or updated rows from the dim table's CDF
SELECT 
    t.tower_id,
    t.network_type,
    SUM(f.cost) as revenue_since_upgrade,
    t._commit_timestamp as upgrade_time
FROM table_changes('telecom_gold.dim_towers1_gold_scd1', 1) t
JOIN telecom_gold.fact_wide_cdr f ON t.tower_id = f.tower_id
--WHERE t._change_type = 'update_postimage' 
--  AND f.timestamp >= t._commit_timestamp
GROUP BY t.tower_id, t.network_type, t._commit_timestamp;

-- COMMAND ----------

-- Handling Soft-Deletes in Downstream Aggregates  
-- Use Case: Cleaning up Gold reporting when a device is retired.
-- If a device is marked as Inactive in your Dimension table (a soft delete), you may need to find all calls associated with that device in the Fact table to flag them or move them to a "Decommissioned" archive.

SELECT 
    f.call_id,
    f.caller_id,
    d.device_id,
    d.status as new_status,
    f.timestamp,d._change_type
FROM telecom_gold.fact_wide_cdr f
JOIN table_changes('telecom_gold.dim_device_gold_scd1', 1) d 
  ON f.device_id = d.device_id
--WHERE d._change_type = 'update_postimage' 
--  AND d.status = 'Inactive';