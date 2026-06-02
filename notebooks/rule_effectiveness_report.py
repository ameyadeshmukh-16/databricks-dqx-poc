# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Rule Effectiveness Report
# MAGIC
# MAGIC Analyses `dq_rule_metrics` — the per-rule violation counts recorded after
# MAGIC every batch and streaming run — to surface:
# MAGIC
# MAGIC 1. **Effectiveness Summary** — total violations, average violation %, last-run count per rule
# MAGIC 2. **Violation Trend** — rule × run timeline (for Databricks chart visualisation)
# MAGIC 3. **Top Offenders** — rules catching the most violations overall
# MAGIC 4. **Zero-fire Candidates** — rules in `dq_rules_registry` with no violations in the last N runs

# COMMAND ----------

# Widgets
dbutils.widgets.dropdown(
    "run_config_name",
    "sales_batch_rules",
    ["sales_batch_rules", "streaming_event_rules", "all"],
    "Rule group to analyse",
)
dbutils.widgets.text("lookback_runs", "10", "Number of recent runs for zero-fire check")

run_config_name = dbutils.widgets.get("run_config_name")
lookback_runs   = int(dbutils.widgets.get("lookback_runs"))

print(f"Run config : {run_config_name}")
print(f"Lookback   : last {lookback_runs} runs (for zero-fire check)")

# COMMAND ----------

import sys
import os

sys.path.insert(0, "/Workspace/Repos/" + os.environ.get("USER", "") + "/databricks-dqx-poc")

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

from config.constants import TABLE_RULE_METRICS, TABLE_RULES

_config_filter = (
    f"run_config_name = '{run_config_name}'"
    if run_config_name != "all"
    else "1=1"
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1 — Effectiveness Summary
# MAGIC
# MAGIC Per rule: total violations across all tracked runs, average violation percentage,
# MAGIC and the violation count from the most recent run.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            rule_name,
            function,
            run_config_name,
            COUNT(DISTINCT run_id)                       AS run_count,
            SUM(violations)                              AS total_violations,
            ROUND(AVG(violation_pct), 2)                 AS avg_violation_pct,
            MAX_BY(violations, recorded_at)              AS last_run_violations,
            MAX_BY(violation_pct, recorded_at)           AS last_run_violation_pct,
            MAX(recorded_at)                             AS last_seen
        FROM {TABLE_RULE_METRICS}
        WHERE {_config_filter}
        GROUP BY rule_name, function, run_config_name
        ORDER BY total_violations DESC
    """)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2 — Violation Trend
# MAGIC
# MAGIC One row per rule per run. Use Databricks' built-in chart view to plot
# MAGIC `violations` over `recorded_at` grouped by `rule_name`.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            rule_name,
            run_type,
            run_config_name,
            violations,
            violation_pct,
            total_records,
            recorded_at
        FROM {TABLE_RULE_METRICS}
        WHERE {_config_filter}
        ORDER BY recorded_at, rule_name
    """)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3 — Top Offenders
# MAGIC
# MAGIC The 10 rules responsible for the most cumulative violations.

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            rule_name,
            function,
            run_config_name,
            SUM(violations)          AS total_violations,
            ROUND(AVG(violation_pct), 2) AS avg_violation_pct
        FROM {TABLE_RULE_METRICS}
        WHERE {_config_filter}
        GROUP BY rule_name, function, run_config_name
        ORDER BY total_violations DESC
        LIMIT 10
    """)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4 — Zero-fire Candidates
# MAGIC
# MAGIC Rules present in `dq_rules_registry` that recorded **zero violations** across
# MAGIC the most recent N runs. These are candidates for review — either the rule is
# MAGIC too permissive, the data has changed, or the rule is no longer relevant.

# COMMAND ----------

# Identify the N most recent distinct run_ids for this config
recent_runs_df = spark.sql(f"""
    SELECT DISTINCT run_id
    FROM {TABLE_RULE_METRICS}
    WHERE {_config_filter}
    ORDER BY MAX(recorded_at) DESC
    LIMIT {lookback_runs}
""")
recent_run_ids = [r["run_id"] for r in recent_runs_df.collect()]

if not recent_run_ids:
    print("No run data found yet. Run the batch or streaming pipeline at least once.")
else:
    run_ids_sql = ", ".join(f"'{r}'" for r in recent_run_ids)
    _reg_filter = (
        f"run_config_name = '{run_config_name}'"
        if run_config_name != "all"
        else "1=1"
    )

    display(
        spark.sql(f"""
            SELECT
                r.name          AS rule_name,
                r.run_config_name,
                r.criticality,
                r.check.function AS function
            FROM {TABLE_RULES} r
            LEFT JOIN (
                SELECT DISTINCT rule_name
                FROM {TABLE_RULE_METRICS}
                WHERE run_id IN ({run_ids_sql})
            ) m ON r.name = m.rule_name
            WHERE {_reg_filter}
              AND m.rule_name IS NULL
            ORDER BY r.criticality DESC, r.name
        """)
    )

# COMMAND ----------

print(f"Report complete. Metrics table: {TABLE_RULE_METRICS}")
