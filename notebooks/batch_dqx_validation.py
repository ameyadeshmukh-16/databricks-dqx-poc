"""
Purpose: 
    Load DQX rules from Delta Table (no YAML at runtime),
    Apply DQX rules to staged sales data,
    Split into clean and quarantine datasets

    **---UPDATED FLOW---**

    Delta Table (dq_rules_registry)
        ↓
    Load rules using DQX
        ↓
    Apply DQX checks
        ↓
    Adds _errors / _warnings columns
        ↓
    Split:
    → sales_clean
    → sales_quarantine
"""

# -----------------------------------
# Imports
# -----------------------------------
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import TableChecksStorageConfig
from pyspark.sql.functions import col, expr
from databricks.connect import DatabricksSession

# -----------------------------------
# Spark Session
# -----------------------------------
spark = DatabricksSession.builder.getOrCreate()

# -----------------------------------
# Initialize DQX Engine
# -----------------------------------
ws = WorkspaceClient()
dqx = DQEngine(ws)

# -----------------------------------
# Load Rules from Delta Table
# -----------------------------------
checks = dqx.load_checks(
    config=TableChecksStorageConfig(
        location="retail_dqx_poc.dq_rules_registry",   # Delta table
        run_config_name="sales_batch_rules"            # logical group
    )
)

print(f"Loaded {len(checks)} rules from Delta table")

# -----------------------------------
# Custom SQL Function (REQUIRED)
# -----------------------------------
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# -----------------------------------
# Load Staged Data
# -----------------------------------
sales_df = spark.table("retail_dqx_poc.sales_staging")

print(f"Input rows: {sales_df.count()}")

# -----------------------------------
# Apply DQX Checks
# -----------------------------------
dq_df = dqx.apply_checks_by_metadata(
    sales_df,
    checks,
    custom_check_functions=custom_check_functions
)

# -----------------------------------
# Split Clean vs Quarantine
# -----------------------------------
good_df = dq_df.filter(col("_errors").isNull())
quarantine_df = dq_df.filter(col("_errors").isNotNull())

# -----------------------------------
# Summary Counts
# -----------------------------------
print(f"Total rows: {dq_df.count()}")
print(f"Clean rows: {good_df.count()}")
print(f"Quarantined rows: {quarantine_df.count()}")

# -----------------------------------
# Persist Clean Data
# -----------------------------------
(good_df.write
    .format("delta")
    .mode("overwrite")  # change to "append" in production
    .option("mergeSchema", "true")
    .saveAsTable("retail_dqx_poc.sales_clean")
)

# -----------------------------------
# Persist Quarantine Data
# -----------------------------------
(quarantine_df.write
    .format("delta")
    .mode("overwrite")  # change to "append" in production
    .option("mergeSchema", "true")
    .saveAsTable("retail_dqx_poc.sales_quarantine")
)

# -----------------------------------
# Preview Quarantine Records
# -----------------------------------
quarantine_df.select(
    "transaction_id",
    "division",
    "customer_id",
    "quantity",
    "unit_price",
    "_errors"
).show(20, truncate=False)

print("DQX validation completed successfully...")