"""
Batch DQX Validation

Load DQX rules from Delta table, apply them to staged sales data,
and split results into clean and quarantine Delta tables.

Flow:
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
from pyspark.sql.functions import col, expr, size
from databricks.connect import DatabricksSession

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
    TABLE_RULES,
    TABLE_STAGING,
    TABLE_SALES_CLEAN,
    TABLE_SALES_QUARANTINE,
    BATCH_RULE_CONFIG_NAME,
    DATABASE,
)
import uuid

# -----------------------------------
# Widgets
# -----------------------------------
dbutils.widgets.text("database", DATABASE, "Target database")
database = dbutils.widgets.get("database")

# -----------------------------------
# Spark Session
# -----------------------------------
spark = DatabricksSession.builder.getOrCreate()

# -----------------------------------
# DQX Engine
# -----------------------------------
ws = WorkspaceClient()
dqx = DQEngine(ws)

# -----------------------------------
# Load Rules from Delta Table
# -----------------------------------
checks = dqx.load_checks(
    config=TableChecksStorageConfig(
        location=TABLE_RULES,
        run_config_name=BATCH_RULE_CONFIG_NAME,
    )
)

logger.info("Loaded %d rules from Delta table", len(checks))

# -----------------------------------
# Custom SQL Function
# -----------------------------------
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# -----------------------------------
# Load Staged Data
# -----------------------------------
sales_df = spark.table(TABLE_STAGING)
logger.info("Input rows: %d", sales_df.count())

# -----------------------------------
# Apply DQX Checks
# -----------------------------------
dq_df = dqx.apply_checks_by_metadata(
    sales_df,
    checks,
    custom_check_functions=custom_check_functions,
)

# -----------------------------------
# Split Clean vs Quarantine
# -----------------------------------
good_df = dq_df.filter(
    (col("_errors").isNull()) | (size(col("_errors")) == 0)
)

quarantine_df = dq_df.filter(
    (col("_errors").isNotNull()) & (size(col("_errors")) > 0)
)

# -----------------------------------
# Summary Counts
# -----------------------------------
total_count      = dq_df.count()
clean_count      = good_df.count()
quarantine_count = quarantine_df.count()

logger.info("Total rows     : %d", total_count)
logger.info("Clean rows     : %d", clean_count)
logger.info("Quarantine rows: %d", quarantine_count)

# -----------------------------------
# Rule Effectiveness Metrics
# -----------------------------------
from ai_suggester.rule_metrics import compute_rule_metrics, write_rule_metrics

_run_id = str(uuid.uuid4())
_metrics = compute_rule_metrics(quarantine_df, total_count, _run_id, "batch", BATCH_RULE_CONFIG_NAME)
_written = write_rule_metrics(_metrics, spark)
logger.info("Rule metrics written: %d rules tracked for run %s", _written, _run_id)

# -----------------------------------
# Persist Clean Data
# -----------------------------------
(
    good_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_SALES_CLEAN)
)

# -----------------------------------
# Persist Quarantine Data
# -----------------------------------
(
    quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(TABLE_SALES_QUARANTINE)
)

logger.info("DQX batch validation completed successfully.")

# -----------------------------------
# Preview Quarantine Records
# -----------------------------------
quarantine_df.select(
    "transaction_id",
    "division",
    "customer_id",
    "quantity",
    "unit_price",
    "_errors",
).show(20, truncate=False)
