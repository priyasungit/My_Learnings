-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Late Arrival Data handling (Very very important for Interviews)
-- MAGIC
-- MAGIC What is the Late-Arriving Data Issue?
-- MAGIC In a high-velocity data pipeline like this Telco Project, we encounter a classic data engineering challenge known as Late-Arriving Dimensions.
-- MAGIC
-- MAGIC **The Problem**
-- MAGIC In an ideal world, when a phone call (Fact) happens, the tower and device (Dimensions) used for that call are already registered in our system. However, in reality:
-- MAGIC
-- MAGIC **Fact is in Speed layer:** CDR (Call Detail Record) logs are streamed in real-time or near-real-time.
-- MAGIC
-- MAGIC **Dimension is in Delay layer:** Information about a new cell tower or a brand-new device model might arrive minutes, hours, or even days later due to batch processing or manual entry.
-- MAGIC
-- MAGIC **The Impact on Gold Layer**
-- MAGIC When our 4_Fact_Medallion_Realtime_Autoload pipeline runs, it performs a LEFT JOIN from the Fact to the Dimensions. If the dimension record isn't there yet: 
-- MAGIC
-- MAGIC - The record is still saved to the Gold table (so we don't lose the call data).
-- MAGIC - All descriptive columns (like tower_name, brand, city) are populated as NULL.
-- MAGIC
-- MAGIC **Instead of re-running the entire stream (which is expensive), we use the Post-Load Merge logic in this notebook. This process:**
-- MAGIC
-- MAGIC - Scans the Gold table for rows where dimension data is NULL.
-- MAGIC
-- MAGIC - Matches them against the newly arrived data in dim_device_gold_scd1 and dim_towers_gold_scd1.
-- MAGIC
-- MAGIC - "Heals" the record by filling in the missing details once they become available.

-- COMMAND ----------

MERGE INTO telecom_gold.fact_wide_cdr AS target
USING telecom_gold.dim_towers_gold_scd1 AS source
ON target.tower_id = source.tower_id
AND (
    target.tower_name IS NULL OR 
    target.city IS NULL OR 
    target.region IS NULL OR 
    target.network_type IS NULL
)
WHEN MATCHED THEN
  UPDATE SET 
    target.tower_name = source.tower_name,
    target.city = source.city,
    target.region = source.region,
    target.network_type = source.network_type;

-- COMMAND ----------

MERGE INTO telecom_gold.fact_wide_cdr AS target
USING telecom_gold.dim_device_gold_scd1 AS source
ON target.device_id = source.device_id
AND (
    target.brand IS NULL OR 
    target.model IS NULL OR 
    target.device_type IS NULL OR 
    target.os IS NULL
)
WHEN MATCHED THEN
  UPDATE SET 
    target.brand = source.brand,
    target.model = source.model,
    target.device_type = source.device_type,
    target.os = source.os;