# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # AI Rule Suggester
# MAGIC
# MAGIC Profiles a Delta table, generates statistical DQX rules automatically,
# MAGIC then calls an LLM (Claude or Azure OpenAI) to suggest business-logic rules
# MAGIC that statistics cannot detect.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Profile the source table
# MAGIC 2. Generate statistical rules
# MAGIC 3. Generate LLM rules
# MAGIC 4. Write all suggestions to `dq_rules_staging` (status = pending)
# MAGIC 5. Review the table below and promote/reject rules via SQL
# MAGIC
# MAGIC **Required cluster env vars (add only the backend you intend to use):**
# MAGIC - `OPENROUTER_API_KEY` + `LLM_BACKEND=openrouter` (if using OpenRouter — free models available)
# MAGIC - `ANTHROPIC_API_KEY` (if using Claude)
# MAGIC - `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT` (if using Azure OpenAI)

# COMMAND ----------

# Widgets — configurable when running as a Databricks Job or interactively
dbutils.widgets.text(
    "source_table",
    "dqx_poc_dev.retail_dqx_poc.sales_staging",
    "Source table to profile",
)
dbutils.widgets.text(
    "run_config_name",
    "sales_batch_rules",
    "Rule group name (for dq_rules_registry)",
)
dbutils.widgets.text(
    "context_description",
    "Retail POS sales transactions from brick-and-mortar stores",
    "Dataset context (plain English, used in LLM prompt)",
)
dbutils.widgets.dropdown(
    "llm_backend",
    "openrouter",
    ["openrouter", "claude", "azure_openai"],
    "LLM backend",
)

source_table       = dbutils.widgets.get("source_table")
run_config_name    = dbutils.widgets.get("run_config_name")
context_description = dbutils.widgets.get("context_description")
llm_backend        = dbutils.widgets.get("llm_backend")

print(f"Source table    : {source_table}")
print(f"Run config name : {run_config_name}")
print(f"LLM backend     : {llm_backend}")
print(f"Context         : {context_description}")

# COMMAND ----------

import sys
import os

# Allow importing project modules from Repos root
sys.path.insert(0, "/Workspace/Repos/" + os.environ.get("USER", "") + "/databricks-dqx-poc")

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Profile the DataFrame

# COMMAND ----------

from ai_suggester.profiler import profile_dataframe

print(f"Loading {source_table}...")
df = spark.table(source_table)
row_count = df.count()
print(f"Loaded {row_count:,} rows, {len(df.columns)} columns")

profile = profile_dataframe(df)

print("\nColumn summary:")
print(f"{'Column':<30} {'Type':<20} {'Null%':>6}  {'Distinct':>8}  {'Sample values'}")
print("-" * 90)
for col_name, stats in profile.items():
    samples = str(stats.get("sample_values", [])[:3])
    print(
        f"{col_name:<30} {stats['dtype']:<20} "
        f"{stats['null_pct'] * 100:>5.1f}%  "
        f"{stats['distinct_count']:>8}  "
        f"{samples}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Generate Statistical Rules

# COMMAND ----------

from ai_suggester.rule_generator import suggest_statistical_rules

stat_rules = suggest_statistical_rules(profile, run_config_name)
print(f"\nStatistical rules generated: {len(stat_rules)}")
for r in stat_rules:
    print(f"  [{r['criticality'].upper()}] {r['name']} — {r['message']} (confidence={r['confidence']:.0%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Generate LLM Rules

# COMMAND ----------

from ai_suggester.llm_rule_suggester import suggest_llm_rules

print(f"\nCalling {llm_backend} LLM...")
try:
    llm_rules = suggest_llm_rules(
        profile=profile,
        run_config_name=run_config_name,
        context=context_description,
        stat_rules=stat_rules,
        backend=llm_backend,
    )
    print(f"LLM rules generated: {len(llm_rules)}")
    for r in llm_rules:
        print(f"  [{r['criticality'].upper()}] {r['name']} — {r['message']}")
except Exception as exc:
    print(f"LLM call failed: {exc}")
    print("Continuing with statistical rules only.")
    llm_rules = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write All Suggestions to Staging

# COMMAND ----------

from ai_suggester.rule_writer import write_suggestions_to_staging

all_rules = stat_rules + llm_rules
written = write_suggestions_to_staging(all_rules, run_config_name, spark)
print(f"\nWrote {written} rules to dq_rules_staging ({len(stat_rules)} statistical + {len(llm_rules)} LLM)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Review Pending Rules
# MAGIC
# MAGIC The table below shows all **pending** suggestions.
# MAGIC Review each row and use the SQL snippets below to approve or reject.

# COMMAND ----------

from config.constants import TABLE_RULES_STAGING

display(
    spark.sql(f"""
        SELECT
            rule_id,
            source,
            criticality,
            name,
            column_name,
            function,
            constraint,
            confidence,
            message,
            reasoning,
            suggested_at
        FROM {TABLE_RULES_STAGING}
        WHERE status = 'pending'
          AND run_config_name = '{run_config_name}'
        ORDER BY source, criticality, name
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Promote or Reject Rules
# MAGIC
# MAGIC Run one of these SQL snippets in a new cell (replace `<rule_id>`):
# MAGIC
# MAGIC ```sql
# MAGIC -- Approve a rule (moves it to dq_rules_registry):
# MAGIC UPDATE retail_dqx_poc.dq_rules_staging
# MAGIC SET status = 'approved',
# MAGIC     reviewed_at = current_timestamp(),
# MAGIC     reviewed_by = 'your.email@company.com'
# MAGIC WHERE rule_id = '<rule_id>';
# MAGIC
# MAGIC -- Reject a rule:
# MAGIC UPDATE retail_dqx_poc.dq_rules_staging
# MAGIC SET status = 'rejected',
# MAGIC     reviewed_at = current_timestamp(),
# MAGIC     reviewed_by = 'your.email@company.com'
# MAGIC WHERE rule_id = '<rule_id>';
# MAGIC ```
# MAGIC
# MAGIC Or use the Python helpers:
# MAGIC ```python
# MAGIC from ai_suggester.rule_writer import promote_rule, reject_rule
# MAGIC promote_rule("<rule_id>", reviewed_by="your.email@company.com", spark=spark)
# MAGIC reject_rule("<rule_id>",  reviewed_by="your.email@company.com", spark=spark)
# MAGIC ```
# MAGIC
# MAGIC After approving, re-run `batch_dqx_validation` — the promoted rule fires immediately
# MAGIC because it is now part of `dq_rules_registry`.

# COMMAND ----------

print("Done. Review the table above, then promote or reject rules using the SQL or Python snippets.")
