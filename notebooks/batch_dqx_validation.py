# Databricks notebook source
# COMMAND ----------
# Title: 02 - Batch DQX Validation
# Purpose: Apply DQX rules to staged sales data, split into clean and quarantine

from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule#, DQRuleColSet
from databricks.sdk import WorkspaceClient
import yaml
from pyspark.sql.functions import col

from dotenv import load_dotenv
import os

load_dotenv()

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
# Load rules from YAML config (versioned in Git)
with open("C:\\Users\\ameya\\databricks-dqx-poc\\configs\\rules_batch.yml", "r") as f:
    rule_config = yaml.safe_load(f)

# COMMAND ----------
# Initialize DQX engine
ws = WorkspaceClient()
dqx = DQEngine(ws)


from pyspark.sql.functions import expr

custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# COMMAND ----------
# Build DQX rule objects from config

"""def build_dqx_rules(rule_config: dict) -> list:
    rules = []
    for r in rule_config["rules"]:
        if r["type"] == "is_not_null":
            rules.append(DQRule.is_not_null(r["column"], name=r["name"], msg=r["message"]))
        elif r["type"] == "is_in_list":
            rules.append(DQRule.is_in_list(r["column"], r["allowed_values"], name=r["name"], msg=r["message"]))
        elif r["type"] == "min_value":
            rules.append(DQRule.min_value(r["column"], r["min"], name=r["name"], msg=r["message"]))
        elif r["type"] == "matches_regex":
            rules.append(DQRule.matches_regex(r["column"], r["regex"], name=r["name"], msg=r["message"]))
        # Add more types as needed
    return rules
"""
"""
def build_dqx_rules(rule_config: dict) -> list:
    rules = []
    for r in rule_config["rules"]:
        if r["type"] == "is_not_null":
            rules.append(DQRule.is_not_null(r["column"], name=r["name"], msg=r["message"]))

        elif r["type"] == "is_in_list":
            allowed_values = r["allowed_values"]
            expr = f"{r['column']} IN ({','.join([repr(v) for v in allowed_values])})"
            rules.append(DQRule.expr(expr, name=r["name"], msg=r["message"]))

        elif r["type"] == "min_value":
            rules.append(DQRule.min_value(r["column"], r["min"], name=r["name"], msg=r["message"]))

        elif r["type"] == "matches_regex":
            rules.append(DQRule.matches_regex(r["column"], r["regex"], name=r["name"], msg=r["message"]))

    return rules
"""
"""
def build_dqx_rules(rule_config: dict) -> list:
    rules = []

    for r in rule_config["rules"]:

        if r["type"] == "is_not_null":
            condition = f"{r['column']} IS NOT NULL"

        elif r["type"] == "is_in_list":
            values = ",".join([f"'{v}'" for v in r["allowed_values"]])
            condition = f"{r['column']} IN ({values})"

        elif r["type"] == "min_value":
            condition = f"{r['column']} >= {r['min']}"

        elif r["type"] == "matches_regex":
            condition = f"{r['column']} RLIKE '{r['regex']}'"

        else:
            continue

        # ✅ IMPORTANT: pass as dictionary (NOT DQRule object)
        rules.append({
            "name": r["name"],
            "check": {"function": "expression", "arguments": {"expression": condition}},
            "columns": [r["column"]],
            "criticality": "error",
            "user_metadata": {
                "message": r["message"]
            }
        })

    return rules
"""

def build_dqx_rules(rule_config: dict) -> list:
    rules = []

    for r in rule_config["rules"]:

        # Default condition builder
        if r["type"] == "is_not_null":
            condition = f"{r['column']} IS NOT NULL"

        elif r["type"] == "is_in_list":
            values = ",".join([f"'{v}'" for v in r["allowed_values"]])
            condition = f"{r['column']} IN ({values})"

        elif r["type"] == "min_value":
            condition = f"{r['column']} >= {r['min']}"

        elif r["type"] == "matches_regex":
            condition = f"{r['column']} RLIKE '{r['regex']}'"

        elif r["type"] == "custom_sql":
            condition = r["sql_expression"]

        else:
            continue

        rules.append({
            "name": r["name"],
            "check": {
                "function": "sql_expression",   # ✅ always use custom
                "arguments": {
                    "expression": condition
                }
            },
            "criticality": (
                "warn" if r.get("criticality", "error") == "warning"
                else r.get("criticality", "error")
            ),
            "user_metadata": {
                "message": r["message"]
            }
        })

    return rules

checks = build_dqx_rules(rule_config)

# COMMAND ----------
# Load staged data
sales_df = spark.table("retail_dqx_poc.sales_staging")
print(f"Input rows: {sales_df.count()}")

# COMMAND ----------
# Apply DQX checks — this is the core DQX call
# apply_checks_by_metadata_and_split returns (good_df, quarantine_df)
#good_df, quarantine_df = dqx.apply_checks_by_metadata_and_split(sales_df, checks, custom_check_functions=custom_check_functions)

# ✅ Clean records = NO errors
#good_df = good_df.filter(col("_errors").isNull())

# ✅ Quarantine = HAS errors
#quarantine_df = quarantine_df.filter(col("_errors").isNotNull())

# Apply DQX checks
dq_df = dqx.apply_checks_by_metadata(
    sales_df,
    checks,
    custom_check_functions=custom_check_functions
)

# ✅ Clean data (NO errors)
good_df = dq_df.filter(col("_errors").isNull())

# ✅ Quarantine data (HAS errors)
quarantine_df = dq_df.filter(col("_errors").isNotNull())

# Counts
print(f"Input rows: {dq_df.count()}")
print(f"Clean rows: {good_df.count()}")
print(f"Quarantined rows: {quarantine_df.count()}")

# COMMAND ----------
# Persist clean data
(good_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("retail_dqx_poc.sales_clean"))

# COMMAND ----------
# Persist quarantine data — DQX adds _dqx_check_status and _dqx_check_failures columns
(quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("retail_dqx_poc.sales_quarantine"))

# COMMAND ----------
# Preview quarantine failure reasons
quarantine_df.select(
    "transaction_id", "division", "customer_id",
    "quantity", "unit_price",
    "_errors"
).show(20, truncate=False)