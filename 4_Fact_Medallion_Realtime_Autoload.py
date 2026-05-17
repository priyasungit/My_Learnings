# Databricks notebook source
# MAGIC %md
# MAGIC ###Fact Table Load - 

# COMMAND ----------

# MAGIC %md
# MAGIC ![](fact_cdr.png)

# COMMAND ----------


# ==========================================
# 1. INITIAL CONFIGURATIONS
# ==========================================
#Replace the below credentials with your own credentials (created using the Automated Cloud Setup).
# Defines the service principal credentials and ADLS Gen2 pathing 
# Required for Auto Loader (CloudFiles) and Delta Lake operations.
tenant_id="089cf941-fd24-44c5-8a92-985784dc42d6"
subscription_id="e68a829e-83e8-42bc-8494-b4ceb80be6c7"
resource_group_name='wd36-rg1'
sp_client_id="530cd74e-90c7-4365-8fed-2d91d79fb006"
sp_secret_scope_name='sp-role'
sp_secret_value="sp-wd36-secret-value1"
storage_account = "wd36adlsstoragedata.dfs.core.windows.net"
container_name="adlscontainer"
folder_name='landing-zone-cdr'

terminate_seconds=200


# COMMAND ----------

# ==========================================
# 1. PATHS & TABLE CONFIGURATIONS
# ==========================================
source_path = f"abfss://{container_name}@{storage_account}/{folder_name}/"
client_secret_value = dbutils.secrets.get(scope=sp_secret_scope_name, key=sp_secret_value)

# Table Names
bronze_table = "telecom_bronze.cdr_bronze_tbl"
silver_table = "telecom_silver.cdr_silver_tbl"
gold_fact_table = "telecom_gold.fact_wide_cdr"
gold_aggr_table = "telecom_gold.fact_realtime_customer_aggr"
dim_tower_scd1 = "telecom_gold.dim_towers_gold_scd1"
dim_device_scd1 = "telecom_gold.dim_device_gold_scd1"
dim_tower_scd2 = "telecom_gold.dim_towers_gold_scd2"
dim_device_scd2 = "telecom_gold.dim_device_gold_scd2"
bronzeschemadir='bronzesch3'
bronzecheckpointdir="bronzeckpt10"
silvercheckpointdir="silverckpt10"
goldcheckpointdir1="gold1ckpt10"
goldcheckpointdir2="gold2ckpt10"

# COMMAND ----------



# Spark Auth Config
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}", sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}", client_secret_value)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Auto Loader Config
cloudFilesConf = {
    "cloudFiles.format": "csv",
    "header": "true",
    "cloudFiles.schemaLocation": f"abfss://{container_name}@{storage_account}/_schemas/{bronzeschemadir}/",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "addNewColumns", # Handles new columns like device_id
    "cloudFiles.useNotifications": "true",
    "cloudFiles.subscriptionId": subscription_id,
    "cloudFiles.resourceGroup": resource_group_name,
    "cloudFiles.tenantId": tenant_id,
    "cloudFiles.clientId": sp_client_id,
    "cloudFiles.clientSecret": client_secret_value,
    "cloudFiles.maxFilesPerTrigger": "10"}

# COMMAND ----------

from pyspark.sql.functions import *
import time

# ==========================================
# 2. BRONZE LAYER (Ingestion)
# ==========================================
df_landing = (spark.readStream
              .format("cloudFiles")
              .options(**cloudFilesConf)
              .load(source_path))

bronze_df = df_landing.withColumn("ingestion_time", current_timestamp())

query_bronze = (bronze_df.writeStream.queryName("Bronze_Ingestion1")
    .format("delta")
    .option("checkpointLocation", f"abfss://{container_name}@{storage_account}/_checkpoints/{bronzecheckpointdir}/")
    .outputMode("append")
    .option("mergeSchema", "true")
    .toTable(bronze_table))

# ==========================================
# 3. SILVER LAYER (Cleaning & Transformation)
# ==========================================
bronze_df_stream = spark.readStream.table(bronze_table)

silver_df = bronze_df_stream \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["call_id"]) \
    .withColumn("call_date", to_date("timestamp")) \
    .withColumn("call_hour", hour("timestamp")) \
    .withColumn("call_status", when(col("duration") > 0, "SUCCESS").otherwise("FAILED")) \
    .withColumn("duration_bucket",
        when(col("duration") < 60, "SHORT")
        .when(col("duration") < 300, "MEDIUM")
        .otherwise("LONG"))

query_silver = (silver_df.writeStream.queryName("Silver_Ingestion1")
    .format("delta")
    .option("checkpointLocation", f"abfss://{container_name}@{storage_account}/_checkpoints/{silvercheckpointdir}/")
    .outputMode("append")
    .option("mergeSchema", "true")
    .toTable(silver_table))

# ==========================================
# 4. GOLD LAYER (Enrichment & Aggregation)
# ==========================================
silver_df_stream = spark.readStream.table(silver_table)

# --- 4a. NEW: Denormalized Enriched Fact ---
# Joining with Dimension tables (Static DataFrames)
dim_device = spark.table(dim_device_scd1) # Or SCD2
dim_tower = spark.table(dim_tower_scd1)

gold_fact_enriched = silver_df_stream.alias("f") \
    .join(dim_device.alias("d"), col("f.device_id") == col("d.device_id"), "left") \
    .join(dim_tower.alias("t"), col("f.tower_id") == col("t.tower_id"), "left") \
    .select(
        "f.*", 
        "d.brand", "d.model", "d.device_type", "d.os", 
        "t.tower_name", "t.city", "t.region", "t.network_type" )

query_gold_fact = (gold_fact_enriched.writeStream.queryName("Gold_enrich_Ingestion1")
    .format("delta")
    .option("checkpointLocation", f"abfss://{container_name}@{storage_account}/_checkpoints/{goldcheckpointdir1}/")
    .outputMode("append")
    .option("mergeSchema", "true")
    .toTable(gold_fact_table))

# --- 4b. Aggregated Metrics ---
gold_aggr_df = silver_df_stream.groupBy("call_date", "caller_id") \
    .agg(
        count("*").alias("total_calls"),
        sum("duration").alias("total_duration"),
        sum("cost").alias("total_revenue"),
        avg("duration").alias("avg_duration")
    )

query_gold_aggr = (gold_aggr_df.writeStream.queryName("Gold_Aggr_Ingestion1")
    .format("delta")
    .option("checkpointLocation", f"abfss://{container_name}@{storage_account}/_checkpoints/{goldcheckpointdir2}/")
    .outputMode("complete")
    .option("mergeSchema", "true")
    .toTable(gold_aggr_table))

active_queries = [query_bronze, query_silver, query_gold_fact, query_gold_aggr]

try:
    print(f"Streaming active for {terminate_seconds} seconds...")
    query_gold_aggr.awaitTermination(terminate_seconds) 
except Exception as e:
    print(f"Timer reached: {e}")

print("Shutting down all streams...")
for q in active_queries:
    if q.isActive:
        q.stop()
        print(f"Stopped: {q.name}")
