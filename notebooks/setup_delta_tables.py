# Purpose: Create Delta tables for clean data, quarantine, and metrics

#Load Environment Variables - Loads values from our .env file
from dotenv import load_dotenv
import os
load_dotenv()

#Create Spark Session - Connect to Databricks cluster using Databricks Connect - allows us to run Spark code from this local notebook and have it execute on the Databricks cluster
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# Set up database and storage access - we will create our Delta tables in the "retail_dqx_poc" database and read/write data from the "databricks-dqx-poc-storage" Azure Data Lake Storage Gen2 account
spark.sql("CREATE DATABASE IF NOT EXISTS retail_dqx_poc")
spark.sql("USE retail_dqx_poc")

# Configure Spark to access Azure Data Lake Storage Gen2 - we set the storage account key in Spark configuration so that we can read/write data to our storage account
spark.conf.set(
  "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net",
  os.environ["AZURE_STORAGE_KEY"]
)

# Load raw CSV into a staging DataFrame - we read the CSV file we generated in the previous step into a Spark DataFrame. This is our raw data that we will apply quality rules to in the next steps.
sales_raw = (spark.read
    .option("header", "true")                           # First row contains column names
    .option("inferSchema", "true")                      # Automatically infer data types for each column
    .csv("abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net/sales_transactions.csv"))

# Write raw DataFrame to a Delta table - we write the raw DataFrame to a Delta table called "sales_staging". This allows us to efficiently store and query the raw data in subsequent steps when we apply our data quality rules.
(sales_raw.write
    .format("delta")                                    # Write data in Delta format for ACID transactions and efficient querying
    .mode("overwrite")                                  # Overwrite the table if it already exists
    .saveAsTable("retail_dqx_poc.sales_staging"))       # Save the DataFrame as a managed Delta table in the "retail_dqx_poc" database with the name "sales_staging".

# Print the number of rows staged into the sales_staging table - this gives us a quick check to confirm that our data was loaded successfully and we have the expected number of records to work with in the next steps.
print(f"Staged {sales_raw.count()} rows into sales_staging")


"""
Right now we have our raw data in the "sales_staging" Delta table. 
In the next steps, 
    we will read from this staging table, 
    apply our data quality rules as defined in rules_batch.yml, 
    and then write the clean records to a "sales_clean" Delta table 
    and the quarantined records to a "sales_quarantine" Delta table. 
    We will also create a "data_quality_metrics" Delta table to store metrics about the data quality checks.

[CSV Raw Data]
       ↓
[Spark DataFrame]
       ↓
[Delta Table → sales_staging]

********************************
Why This Step is Critical
********************************
Because:

DQX NEVER works directly on raw files       ->      It works on DataFrames / Tables

"""