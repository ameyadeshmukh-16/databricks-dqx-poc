"""
Review Rule — approve or reject a pending rule suggestion (human-in-the-loop).

Run from the Jobs UI: "[RTM] Review Rule" > Run now with different parameters.
(The live board's Approve / Reject buttons run the very same logic, see src/review_service.py.)

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

from pyspark.sql import SparkSession

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(inspect.currentframe().f_code.co_filename)))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from config.constants import TABLE_REVIEW_NOTIFICATIONS
from src.review_service import run_review
from src.rule_review import ReviewError, format_pending


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


def show_pending(pending: list[dict]) -> None:
    print(f"\nPENDING SUGGESTIONS for {run_config_name}: {len(pending)}")
    print(format_pending(pending))


def show_notifications(limit: int = 3) -> None:
    if not spark.catalog.tableExists(TABLE_REVIEW_NOTIFICATIONS):
        return
    rows = spark.table(TABLE_REVIEW_NOTIFICATIONS).filter(
        f"run_config_name = '{run_config_name}'").orderBy("created_at", ascending=False).limit(limit).collect()
    print(f"\nLATEST NOTIFICATIONS ({len(rows)})")
    for r in rows:
        print(f"  {r['created_at']}  +{r['new_count']} new, {r['pending_total']} pending: {', '.join(r['rule_names'])}")


result = run_review(spark, action, rule_ref, run_config_name)
print(f"Reviewer: {result['reviewer']} | action: {action} | group: {run_config_name}")

if not result["ok"]:
    print(f"\nREFUSED: {result['message']}")
    raise ReviewError(result["message"])

if action == "list":
    show_pending(result["pending"])
    show_notifications()
else:
    rule = result["rule"]
    print(f"\nSelected: {rule['name']} ({rule['rule_id']})")
    print(result["message"])
    audit, live = result["audit"], result["live"]
    if audit:
        print(f"\nAUDIT TRAIL  {audit['name']}: status={audit['status']}, "
              f"reviewed_by={audit['reviewed_by']}, reviewed_at={audit['reviewed_at']}")
    if live:
        print(f"LIVE IN REGISTRY  {live['name']}: {live['function']} {live['arguments']} criticality={live['criticality']}")
    else:
        print(f"NOT IN REGISTRY  {rule['name']} (never enforced)")
    show_pending(result["pending"])
