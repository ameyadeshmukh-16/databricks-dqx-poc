# Databricks notebook source
# COMMAND ----------
# Title: 01 - Setup Delta Tables
# Purpose: Create Delta tables for clean data, quarantine, and metrics
#from pyspark.sql import SparkSession
#import os

from dotenv import load_dotenv
import os

load_dotenv()

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

#from databricks.connect import DatabricksSession
#spark = SparkSession.builder.appName("SetupDeltaTables").getOrCreate()
#spark = DatabricksSession.builder.getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS retail_dqx_poc")
spark.sql("USE retail_dqx_poc")


spark.conf.set(
  "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net",
  os.environ["AZURE_STORAGE_KEY"]
)

# Load raw CSV into a staging DataFrame
sales_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net/sales_transactions.csv"))

# Persist as Delta (staging — no quality rules applied yet)
(sales_raw.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("retail_dqx_poc.sales_staging"))

print(f"Staged {sales_raw.count()} rows into sales_staging")