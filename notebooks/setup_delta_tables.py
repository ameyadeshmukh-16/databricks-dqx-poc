"""
Setup Delta Tables — Stage raw CSV data for batch DQX validation.

Reads the raw sales CSV from ADLS Gen2 and writes it to the
sales_staging Delta table. Run this once before batch_dqx_validation.py.
"""

# -----------------------------------
# Imports
# -----------------------------------
import os
from dotenv import load_dotenv
from databricks.connect import DatabricksSession

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
import inspect as _inspect, os as _os, sys as _sys
_repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(_inspect.currentframe().f_code.co_filename)))
if _repo_root not in _sys.path:
    _sys.path.insert(0, _repo_root)

# -----------------------------------
# Logging
# -----------------------------------
from src.logger import get_logger
logger = get_logger(__name__)

# -----------------------------------
# Constants
# -----------------------------------
from config.constants import (
    DATABASE,
    TABLE_STAGING,
    ADLS_BASE,
    STORAGE_ACCOUNT,
)

# -----------------------------------
# Spark Session
# -----------------------------------
spark = DatabricksSession.builder.getOrCreate()

# -----------------------------------
# Storage Config
# -----------------------------------
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"],
)

# -----------------------------------
# Widgets
# -----------------------------------
# Allow overriding the CSV path and database when running as a Databricks Job.
dbutils.widgets.text("csv_path", f"{ADLS_BASE}/sales_transactions.csv", "ADLS CSV path")
dbutils.widgets.text("database", DATABASE, "Target database")

csv_path = dbutils.widgets.get("csv_path")
database = dbutils.widgets.get("database")

# -----------------------------------
# Create Database
# -----------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")
logger.info("Using database: %s", database)

# -----------------------------------
# Load CSV → Staging
# -----------------------------------
logger.info("Reading CSV from: %s", csv_path)

sales_raw = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csv_path)
)

(
    sales_raw.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TABLE_STAGING)
)

logger.info("Staged %d rows into %s", sales_raw.count(), TABLE_STAGING)
