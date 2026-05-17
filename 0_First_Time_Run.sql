-- Databricks notebook source
-- uncomment only for the first time
/*CREATE OR REPLACE CONNECTION telecom_gcp_mysql_connection
TYPE MYSQL
OPTIONS (
  host '35.223.80.16',
  port '3306',
  user 'inceptez',
  password 'Inceptez@123'--we can better store this password in a DBC secret scope/keyvault
);*/

-- COMMAND ----------

CREATE FOREIGN CATALOG IF NOT EXISTS telecom_foreign_catalog
USING CONNECTION telecom_gcp_mysql_connection;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS telecom_bronze;
CREATE SCHEMA IF NOT EXISTS telecom_silver;
CREATE SCHEMA IF NOT EXISTS telecom_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####To perform complete reload of the dim tables in the Source DB side, follow the steps.
-- MAGIC **Goto https://inceptezlabs.com/telco -> DB Data Load -> Initial load and subsequent loads (as per the instructions given)**

-- COMMAND ----------

--If we want to reload the entire dim_devices tables from scratch
TRUNCATE TABLE telecom_bronze.dim_device_bronze;
TRUNCATE TABLE telecom_silver.dim_device_silver;
TRUNCATE TABLE telecom_gold.dim_device_gold_scd1;
TRUNCATE TABLE telecom_gold.dim_device_gold_scd2;


-- COMMAND ----------

--If we want to reload the entire dim_tower tables from scratch
TRUNCATE TABLE telecom_bronze.bronze_dim_tower;
DELETE FROM telecom_gold.dim_towers1_gold_scd1;
DELETE FROM telecom_gold.dim_towers1_gold_scd2;
--To reload silver and gold tables, we have to do a "Run Pipeline with Full Table Refresh" in the declarative pipeline "2_Pipeline_Dim_Towers_DLT_SCD_CDF_Silver_Gold"


-- COMMAND ----------

TRUNCATE TABLE telecom_bronze.cdr_bronze_tbl;
TRUNCATE TABLE telecom_silver.cdr_silver_tbl;
TRUNCATE TABLE telecom_gold.fact_wide_cdr;
TRUNCATE TABLE telecom_gold.fact_realtime_customer_aggr;

-- COMMAND ----------

/*drop table telecom_bronze.cdr_bronze_tbl;
drop table telecom_silver.cdr_silver_tbl;
drop table telecom_gold.fact_wide_cdr;
drop table telecom_gold.fact_realtime_customer_aggr;*/