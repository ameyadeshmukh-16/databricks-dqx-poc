"""
Review Rule — approve or reject a pending rule suggestion (human-in-the-loop).

Run from the Jobs UI: "[RTM] Review Rule" > Run now with different parameters.

Parameters (spark_python_task argv, from the job parameters):
  --action=list|approve|reject     (default list)
  --rule=<rule name or rule_id>
  --run_config_name=streaming_event_rules

Approve checks the rule first (its column must exist in the target table and DQX
must accept the translated check), then promotes it into dq_rules_registry; a
running stream picks it up on its next micro-batch. The reviewer recorded in the
audit trail is the user the job runs as, not free text.
"""

# -----------------------------------
# Imports
# -----------------------------------
import inspect
import os
import sys

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from ai_suggester.rule_writer import promote_rule, reject_rule
from config.constants import TABLE_RULES, TABLE_RULES_STAGING, TABLE_REVIEW_NOTIFICATIONS
from src.rule_review import (
    TARGET_TABLE_BY_RUN_CONFIG,
    ReviewError,
    check_column,
    format_pending,
    resolve_rule,
    staged_to_check_dict,
)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


action          = _arg("action", "list").strip().lower()
rule_ref        = _arg("rule", "")
run_config_name = _arg("run_config_name", "streaming_event_rules").strip()

spark = SparkSession.builder.getOrCreate()
reviewer = spark.sql("SELECT current_user()").first()[0]


def pending_rows() -> list[dict]:
    if not spark.catalog.tableExists(TABLE_RULES_STAGING):
        return []
    rows = (
        spark.table(TABLE_RULES_STAGING)
        .filter(f"run_config_name = '{run_config_name}' AND status = 'pending'")
        .orderBy("source", "name")
        .collect()
    )
    return [r.asDict() for r in rows]


def show_pending() -> None:
    rows = pending_rows()
    print(f"\nPENDING SUGGESTIONS for {run_config_name}: {len(rows)}")
    print(format_pending(rows))


def show_notifications(limit: int = 3) -> None:
    if not spark.catalog.tableExists(TABLE_REVIEW_NOTIFICATIONS):
        return
    rows = spark.table(TABLE_REVIEW_NOTIFICATIONS).filter(
        f"run_config_name = '{run_config_name}'").orderBy("created_at", ascending=False).limit(limit).collect()
    print(f"\nLATEST NOTIFICATIONS ({len(rows)})")
    for r in rows:
        print(f"  {r['created_at']}  +{r['new_count']} new, {r['pending_total']} pending: {', '.join(r['rule_names'])}")


def show_audit(rule_id: str, name: str) -> None:
    audit = spark.table(TABLE_RULES_STAGING).filter(f"rule_id = '{rule_id}'").first()
    print(f"\nAUDIT TRAIL  {audit['name']}: status={audit['status']}, "
          f"reviewed_by={audit['reviewed_by']}, reviewed_at={audit['reviewed_at']}")
    live = spark.table(TABLE_RULES).filter(f"run_config_name = '{run_config_name}' AND name = '{name}'").first()
    if live:
        print(f"LIVE IN REGISTRY  {live['name']}: {live['check']['function']} "
              f"{dict(live['check']['arguments'])} criticality={live['criticality']}")
    else:
        print(f"NOT IN REGISTRY  {name} (never enforced)")


print(f"Reviewer: {reviewer} | action: {action} | group: {run_config_name}")

try:
    if action == "list":
        show_pending()
        show_notifications()

    elif action in ("approve", "reject"):
        row = resolve_rule(pending_rows(), rule_ref)
        print(f"\nSelected: {row['name']} ({row['rule_id']})")

        if action == "approve":
            target = TARGET_TABLE_BY_RUN_CONFIG.get(run_config_name)
            if target and spark.catalog.tableExists(target):
                check_column(row["column_name"], spark.table(target).columns)

            status = DQEngine(WorkspaceClient()).validate_checks([staged_to_check_dict(row)])
            if status.has_errors:
                raise ReviewError(f"DQX rejected this rule: {status.errors}")

            promote_rule(row["rule_id"], reviewer, spark)
            print("APPROVED — a running stream applies it on its next micro-batch (~30 s).")
        else:
            reject_rule(row["rule_id"], reviewer, spark)
            print("REJECTED — it will never be enforced.")

        show_audit(row["rule_id"], row["name"])
        show_pending()

    else:
        raise ReviewError(f"Unknown action '{action}'. Use list, approve or reject.")

except ReviewError as exc:
    print(f"\nREFUSED: {exc}")
    raise
