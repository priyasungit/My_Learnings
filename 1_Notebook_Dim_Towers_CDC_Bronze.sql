-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Dimension Table (Towers) Load - CDC, SCD1, SCD2, CDF, Reporting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](towers_cdc.png)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS telecom_bronze.bronze_dim_tower (
    tower_id STRING,
    tower_name STRING,
    city STRING,
    state STRING,
    region STRING,
    network_type STRING,
    installation_date DATE,
    updated_at TIMESTAMP
);

-- COMMAND ----------

-- Incremental load applying Watermarking Feature
INSERT INTO telecom_bronze.bronze_dim_tower
SELECT 
    tower_id, 
    tower_name, 
    city, 
    state, 
    region, 
    network_type, 
    installation_date, 
    updated_at
FROM telecom_foreign_catalog.telco_schema.dim_tower_source
WHERE updated_at > (
    SELECT COALESCE(MAX(updated_at), '1900-01-01') 
    FROM telecom_bronze.bronze_dim_tower
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Further proceed to Lakeflow Declarative pipeline - 2_Pipeline_DLT_SCD_CDF_Silver_Gold

-- COMMAND ----------

select * from telecom_bronze.bronze_dim_tower