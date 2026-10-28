# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Data Quality Alert Check
# MAGIC
# MAGIC Compares each rule's most recent `violation_pct` (from `dq_rule_metrics`)
# MAGIC against its own historical baseline. Rules that spike beyond the configured
# MAGIC threshold trigger a Slack notification and are logged to `dq_alert_log`.
# MAGIC
# MAGIC **Required cluster secret:**
# MAGIC - `SLACK_WEBHOOK_URL` — read from a Databricks secret scope, not a plaintext parameter.
# MAGIC   Set up: `databricks secrets create-scope dqx-poc` then
# MAGIC   `databricks secrets put-secret dqx-poc slack-webhook-url`
# MAGIC
# MAGIC   The `dqx-poc` scope is backed by Azure Key Vault, whose secret names allow only letters,
# MAGIC   digits and hyphens, so the key is `slack-webhook-url` (create it in the Key Vault).
# MAGIC   The old underscore name `slack_webhook_url` is still tried as a fallback.

# COMMAND ----------

# Widgets — configurable when running as a Databricks Job or interactively
dbutils.widgets.dropdown(
    "run_config_name",
    "sales_batch_rules",
    ["sales_batch_rules", "streaming_event_rules"],
    "Rule group to check",
)
dbutils.widgets.text("lookback_runs", "10", "Runs used to compute baseline")
dbutils.widgets.text("secret_scope", "dqx-poc", "Databricks secret scope holding the webhook URL")

run_config_name = dbutils.widgets.get("run_config_name")
lookback_runs   = int(dbutils.widgets.get("lookback_runs"))
secret_scope    = dbutils.widgets.get("secret_scope")

print(f"Run config    : {run_config_name}")
print(f"Lookback runs : {lookback_runs}")
print(f"Secret scope  : {secret_scope}")

# COMMAND ----------

import sys
import os

# Repo root on sys.path: derive it from this notebook's own workspace path
# (works for bundle deployments under .../files/ as well as Repos / Git folders).
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_repo_root = os.path.dirname(os.path.dirname("/Workspace" + _nb_path))
sys.path.insert(0, _repo_root)

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Load Slack Webhook URL

# COMMAND ----------

webhook_url = ""
last_error = "no value found"
for _key in ("slack-webhook-url", "slack_webhook_url"):        # Key Vault-friendly name first
    try:
        webhook_url = dbutils.secrets.get(scope=secret_scope, key=_key)
        print(f"Loaded the Slack webhook from secret '{secret_scope}/{_key}'.")
        break
    except Exception as exc:                                    # noqa: BLE001 - try the next name
        last_error = exc
if not webhook_url:
    print(f"Could not read a Slack webhook from scope '{secret_scope}' (tried slack-webhook-url, slack_webhook_url): {last_error}")
    print("Spikes will still be detected and logged, but no Slack notification will be sent.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Detect Spikes and Notify

# COMMAND ----------

from ai_suggester.alerting import run_alert_check

spikes = run_alert_check(
    spark=spark,
    run_config_name=run_config_name,
    webhook_url=webhook_url,
    lookback_runs=lookback_runs,
)

if not spikes:
    print("No spikes detected — all rules within their historical baseline.")
else:
    print(f"{len(spikes)} spike(s) detected:\n")
    for s in spikes:
        print(
            f"  [{s['rule_name']}] {s['last_run_pct']}% this run vs. "
            f"{s['baseline_pct']}% baseline (run_id={s['run_id']})"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Recent Alert History

# COMMAND ----------

from config.constants import TABLE_ALERT_LOG

display(
    spark.sql(f"""
        SELECT rule_name, run_config_name, baseline_pct, last_run_pct,
               multiplier_used, triggered_at, notified, notify_error
        FROM {TABLE_ALERT_LOG}
        WHERE run_config_name = '{run_config_name}'
        ORDER BY triggered_at DESC
        LIMIT 50
    """)
)

# COMMAND ----------

print("Alert check complete.")
