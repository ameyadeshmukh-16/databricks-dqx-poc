# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Quarantine Analyzer
# MAGIC
# MAGIC Mines a DQX quarantine table for recurring violation patterns,
# MAGIC then calls an LLM to suggest new data quality rules that close coverage gaps.
# MAGIC
# MAGIC **Workflow:**
# MAGIC 1. Load the quarantine table
# MAGIC 2. Extract top violation patterns from `_errors`
# MAGIC 3. Send patterns to an LLM for new rule suggestions
# MAGIC 4. Write suggestions to `dq_rules_staging` (status = pending)
# MAGIC 5. Review below and promote/reject rules
# MAGIC
# MAGIC **Required cluster env vars (add only the backend you intend to use):**
# MAGIC - `OPENROUTER_API_KEY` + `LLM_BACKEND=openrouter` (free models available)
# MAGIC - `ANTHROPIC_API_KEY` (if using Claude)
# MAGIC - `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT` (if using Azure OpenAI)

# COMMAND ----------

# Widgets — configurable when running as a Databricks Job or interactively
dbutils.widgets.text(
    "quarantine_table",
    "dqx_poc_cluster_7405619883480512.retail_dqx_poc.sales_quarantine",
    "Quarantine table to analyse",
)
dbutils.widgets.text(
    "run_config_name",
    "sales_batch_rules",
    "Rule group name (for dq_rules_staging suggestions)",
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
dbutils.widgets.text(
    "top_n",
    "20",
    "Top N violation patterns to surface",
)

quarantine_table    = dbutils.widgets.get("quarantine_table")
run_config_name     = dbutils.widgets.get("run_config_name")
context_description = dbutils.widgets.get("context_description")
llm_backend         = dbutils.widgets.get("llm_backend")
top_n               = int(dbutils.widgets.get("top_n"))

print(f"Quarantine table : {quarantine_table}")
print(f"Run config name  : {run_config_name}")
print(f"LLM backend      : {llm_backend}")
print(f"Top N patterns   : {top_n}")
print(f"Context          : {context_description}")

# COMMAND ----------

import sys
import os

sys.path.insert(0, "/Workspace/Repos/" + os.environ.get("USER", "") + "/databricks-dqx-poc")

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load Quarantine Table

# COMMAND ----------

print(f"Loading {quarantine_table}...")
df = spark.table(quarantine_table)
total_rows = df.count()
print(f"Loaded {total_rows:,} quarantined records, {len(df.columns)} columns")

if total_rows == 0:
    print("⚠️  Quarantine table is empty. Run a batch or streaming pipeline first to populate it.")
    dbutils.notebook.exit("Quarantine table is empty — nothing to analyse")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Extract Violation Patterns

# COMMAND ----------

from ai_suggester.quarantine_analyzer import extract_violation_patterns

patterns = extract_violation_patterns(df, top_n=top_n)

print(f"\nTotal quarantined records : {patterns['total_quarantined']:,}")
print(f"Distinct violation patterns: {len(patterns['violation_counts'])}")
print()
print(f"{'#':<4} {'Rule name':<45} {'Count':>7}  {'% of quarantine':>15}")
print("-" * 76)
for i, v in enumerate(patterns["violation_counts"], 1):
    print(
        f"{i:<4} {v['rule_name']:<45} {v['count']:>7}  {v['pct_of_quarantine']:>14.1f}%"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Generate LLM Rule Suggestions

# COMMAND ----------

from ai_suggester.quarantine_analyzer import suggest_rules_from_quarantine

print(f"\nCalling {llm_backend} LLM with {len(patterns['violation_counts'])} patterns...")
try:
    quarantine_rules = suggest_rules_from_quarantine(
        patterns=patterns,
        run_config_name=run_config_name,
        context=context_description,
        backend=llm_backend,
    )
    print(f"LLM rule suggestions: {len(quarantine_rules)}")
    for r in quarantine_rules:
        print(f"  [{r['criticality'].upper()}] {r['name']} — {r['message']}")
except Exception as exc:
    print(f"LLM call failed: {exc}")
    print("Exiting without writing any suggestions.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Write Suggestions to Staging

# COMMAND ----------

from ai_suggester.rule_writer import write_suggestions_to_staging

written = write_suggestions_to_staging(quarantine_rules, run_config_name, spark)
print(f"\nWrote {written} quarantine-derived rule suggestions to dq_rules_staging")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Review Pending Suggestions
# MAGIC
# MAGIC The table below shows all **pending** quarantine-derived suggestions.
# MAGIC Use the Python helpers below to promote or reject each rule.

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
            message,
            reasoning,
            suggested_at
        FROM {TABLE_RULES_STAGING}
        WHERE status = 'pending'
          AND run_config_name = '{run_config_name}'
          AND source = 'quarantine_analysis'
        ORDER BY criticality, name
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Promote or Reject Rules
# MAGIC
# MAGIC Use the Python helpers (replace `<rule_id>` with the UUID from the table above):
# MAGIC
# MAGIC ```python
# MAGIC from ai_suggester.rule_writer import promote_rule, reject_rule
# MAGIC
# MAGIC # Approve — moves rule into dq_rules_registry and enforces it on next run:
# MAGIC promote_rule("<rule_id>", reviewed_by="your.email@company.com", spark=spark)
# MAGIC
# MAGIC # Reject — marks rule as rejected, never enters registry:
# MAGIC reject_rule("<rule_id>",  reviewed_by="your.email@company.com", spark=spark)
# MAGIC ```
# MAGIC
# MAGIC After promoting, re-run the batch or streaming pipeline — the new rule fires immediately
# MAGIC because it is now part of `dq_rules_registry`.

# COMMAND ----------

print("Done. Review the table above, then promote or reject rules using the Python snippets.")
