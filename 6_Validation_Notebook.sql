-- Databricks notebook source
select * from telecom_silver.dim_device_silver

-- COMMAND ----------

select * from telecom_gold.dim_device_gold_scd1

-- COMMAND ----------

select * from telecom_gold.dim_device_gold_scd2;

-- COMMAND ----------

select * from telecom_gold.dim_towers1_gold_scd1

-- COMMAND ----------

select * from telecom_gold.dim_towers1_gold_scd2

-- COMMAND ----------

select * from telecom_gold.fact_wide_cdr-- where caller_id=9840800131

-- COMMAND ----------

select * from telecom_gold.fact_realtime_customer_aggr-- where caller_id=9840800131;