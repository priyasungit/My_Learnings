-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Dimension Table (Towers) Load - CDC, SCD1, SCD2, CDF, Reporting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](devices_cdc.png)

-- COMMAND ----------

  -- Bronze Table
CREATE TABLE IF NOT EXISTS telecom_bronze.dim_device_bronze (
  device_id INT, device_type STRING, brand STRING, model STRING, os STRING, 
  owner_customer_id INT, status STRING, updated_at TIMESTAMP);
  -- Silver Table
CREATE TABLE IF NOT EXISTS telecom_silver.dim_device_silver (
    device_id STRING, brand STRING, model STRING, os STRING, 
    device_type STRING, owner_customer_id STRING, status STRING, updated_at TIMESTAMP
) USING DELTA;

-- Gold SCD1 Table (CDF Enabled)
CREATE TABLE IF NOT EXISTS telecom_gold.dim_device_gold_scd1 (
    device_id STRING, brand STRING, model STRING, os STRING, 
    device_type STRING, owner_customer_id STRING, status STRING, updated_at TIMESTAMP
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Gold SCD2 Table (Includes DLT-style history columns)
CREATE TABLE IF NOT EXISTS telecom_gold.dim_device_gold_scd2 (
    device_id STRING, brand STRING, model STRING, os STRING, 
    device_type STRING, owner_customer_id STRING, status STRING, updated_at TIMESTAMP,
    start_at TIMESTAMP, end_at TIMESTAMP
) USING DELTA;

-- COMMAND ----------

select * from telecom_bronze.dim_device_bronze limit 10;

-- COMMAND ----------

--1. CDC (History load for the first time alone) applying Watermarking Feature using Foreign Catalog
INSERT INTO telecom_bronze.dim_device_bronze
SELECT 
    device_id, 
    device_type, 
    brand, 
    model, 
    os, 
    owner_customer_id, 
    status, 
    updated_at
FROM telecom_foreign_catalog.telco_schema.dim_device
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') 
    FROM telecom_bronze.dim_device_bronze);
    --This is checkpointing or watermarking feature to achieve incremental load/CDC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, upper, trim, when, row_number
-- MAGIC from pyspark.sql.window import Window
-- MAGIC
-- MAGIC # ====================================================================
-- MAGIC # 1. READ BRONZE & LOAD CURATED SILVER (BATCH)
-- MAGIC # ====================================================================
-- MAGIC
-- MAGIC raw_df = spark.read.table("telecom_bronze.dim_device_bronze")
-- MAGIC # Apply filters and transformations
-- MAGIC silver_df = (
-- MAGIC     raw_df
-- MAGIC     .filter(col("device_id").isNotNull()) 
-- MAGIC     .withColumn("device_id", col("device_id").cast("string"))
-- MAGIC     .withColumn("owner_customer_id", col("owner_customer_id").cast("string"))
-- MAGIC     .withColumn("brand", upper(trim(col("brand"))))
-- MAGIC     .withColumn("model", trim(col("model")))
-- MAGIC     .withColumn("os", when(col("os").rlike("(?i)^ios$"), "iOS")
-- MAGIC                       .otherwise(upper(trim(col("os"))))))
-- MAGIC
-- MAGIC silver_df.write.format("delta").mode("append").saveAsTable("telecom_silver.dim_device_silver")
-- MAGIC
-- MAGIC # ====================================================================
-- MAGIC # 2. PREPARE LATEST UPDATES FOR MERGE
-- MAGIC # ====================================================================
-- MAGIC # Deduplicate to merge only latest updated device
-- MAGIC
-- MAGIC latest_updates_df = (
-- MAGIC     silver_df
-- MAGIC     .withColumn("rn", row_number().over(Window.partitionBy("device_id").orderBy(col("updated_at").desc())))
-- MAGIC     .filter(col("rn") == 1)
-- MAGIC     .drop("rn"))
-- MAGIC
-- MAGIC latest_updates_df.createOrReplaceTempView("dedup_latest_silver")
-- MAGIC
-- MAGIC # ====================================================================
-- MAGIC # 3. GOLD LAYER: SCD TYPE 1 (Hard Deletes + Hash Comparison)
-- MAGIC # ====================================================================
-- MAGIC spark.sql("""
-- MAGIC     MERGE INTO telecom_gold.dim_device_gold_scd1 t
-- MAGIC     USING dedup_latest_silver s
-- MAGIC     ON t.device_id = s.device_id
-- MAGIC     
-- MAGIC     WHEN MATCHED AND s.status = 'Inactive' THEN 
-- MAGIC         DELETE -- Hard delete
-- MAGIC         
-- MAGIC     WHEN MATCHED AND s.updated_at > t.updated_at 
-- MAGIC                  AND hash(s.device_type, s.brand, s.model, s.os, s.owner_customer_id, s.status) <> 
-- MAGIC                      hash(t.device_type, t.brand, t.model, t.os, t.owner_customer_id, t.status) THEN 
-- MAGIC         UPDATE SET *
-- MAGIC         
-- MAGIC     WHEN NOT MATCHED AND s.status != 'Inactive' THEN 
-- MAGIC         INSERT *
-- MAGIC """)
-- MAGIC
-- MAGIC # ====================================================================
-- MAGIC # 4. GOLD LAYER: SCD TYPE 2 (Hash Comparison)
-- MAGIC # ====================================================================
-- MAGIC # Create a staging table that marks what needs to be closed and what needs to be inserted
-- MAGIC # We use NULL as a merge_key for new records to force the "NOT MATCHED" path
-- MAGIC staged_updates = spark.sql("""
-- MAGIC     SELECT s.*, s.device_id as merge_key FROM dedup_latest_silver s
-- MAGIC     UNION ALL
-- MAGIC     SELECT s.*, NULL as merge_key FROM dedup_latest_silver s
-- MAGIC     JOIN telecom_gold.dim_device_gold_scd2 t ON s.device_id = t.device_id
-- MAGIC     WHERE t.end_at IS NULL 
-- MAGIC       AND hash(s.device_type, s.brand, s.model, s.os, s.owner_customer_id, s.status) <> 
-- MAGIC           hash(t.device_type, t.brand, t.model, t.os, t.owner_customer_id, t.status)
-- MAGIC """)
-- MAGIC
-- MAGIC staged_updates.createOrReplaceTempView("staged_updates")
-- MAGIC
-- MAGIC spark.sql("""
-- MAGIC     MERGE INTO telecom_gold.dim_device_gold_scd2 t
-- MAGIC     USING staged_updates s
-- MAGIC     ON t.device_id = s.merge_key AND t.end_at IS NULL
-- MAGIC     
-- MAGIC     /* 1. If we matched the merge_key and the hash is different, CLOSE the old record */
-- MAGIC     WHEN MATCHED AND hash(s.device_type, s.brand, s.model, s.os, s.owner_customer_id, s.status) <> 
-- MAGIC                      hash(t.device_type, t.brand, t.model, t.os, t.owner_customer_id, t.status)
-- MAGIC     THEN UPDATE SET t.end_at = s.updated_at
-- MAGIC     
-- MAGIC     /* 2. If we didn't match (merge_key was NULL or ID is brand new), INSERT the new record */
-- MAGIC     WHEN NOT MATCHED THEN
-- MAGIC     INSERT (device_id, device_type, brand, model, os, owner_customer_id, status, updated_at, start_at, end_at)
-- MAGIC     VALUES (s.device_id, s.device_type, s.brand, s.model, s.os, s.owner_customer_id, s.status, s.updated_at, s.updated_at, NULL)
-- MAGIC """)