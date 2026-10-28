"""
Status Snapshot — a one-click, read-only view of the streaming loop.

Run from the Jobs UI: "[RTM] Live Status" > Run now. Reads the output; changes nothing.

Parameters (spark_python_task argv):
  --run_config_name=streaming_event_rules
  --last_batches=5
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

from config.constants import (
    TABLE_BATCH_METRICS,
    TABLE_REVIEW_NOTIFICATIONS,
    TABLE_RULE_METRICS,
    TABLE_RULES,
    TABLE_RULES_STAGING,
    TABLE_STREAM_CLEAN,
    TABLE_STREAM_QUARANTINE,
)


def _arg(name: str, default: str) -> str:
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return default


run_config_name = _arg("run_config_name", "streaming_event_rules")
last_batches    = int(_arg("last_batches", "5"))

spark = SparkSession.builder.getOrCreate()


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def exists(table: str) -> bool:
    return spark.catalog.tableExists(table)


def show(sql: str, empty_msg: str = "  (nothing yet)") -> None:
    rows = spark.sql(sql).collect()
    if not rows:
        print(empty_msg)
        return
    cols = rows[0].__fields__
    print("  " + " | ".join(cols))
    for r in rows:
        print("  " + " | ".join(str(r[c]) for c in cols))


section("Event flow")
for label, table in (("clean", TABLE_STREAM_CLEAN), ("quarantine", TABLE_STREAM_QUARANTINE)):
    print(f"  {label:<11}: {spark.table(table).count() if exists(table) else 0} rows")
if exists(TABLE_BATCH_METRICS):
    print(f"\n  last {last_batches} micro-batches:")
    show(f"SELECT batch_id, clean_count, quarantine_count, total_count, processed_at "
         f"FROM {TABLE_BATCH_METRICS} ORDER BY batch_id DESC LIMIT {last_batches}")

section(f"Active rules ({run_config_name})")
if exists(TABLE_RULES):
    show(f"SELECT name, `check`.function AS function, criticality, created_at FROM {TABLE_RULES} "
         f"WHERE run_config_name = '{run_config_name}' ORDER BY created_at, name")
else:
    print("  (registry not created yet)")

section("Rules firing in the stream (first seen = when a rule first caught an event)")
if exists(TABLE_RULE_METRICS):
    show(f"SELECT rule_name, MIN(recorded_at) AS first_seen, COUNT(*) AS batches_fired, "
         f"MAX_BY(violation_pct, recorded_at) AS last_batch_pct "
         f"FROM {TABLE_RULE_METRICS} WHERE run_config_name = '{run_config_name}' AND run_type = 'streaming' "
         f"GROUP BY rule_name ORDER BY first_seen")
else:
    print("  (no metrics yet)")

section("Suggestions waiting for review")
if exists(TABLE_RULES_STAGING):
    show(f"SELECT name, function, criticality, source, confidence FROM {TABLE_RULES_STAGING} "
         f"WHERE run_config_name = '{run_config_name}' AND status = 'pending' ORDER BY source, name")
    show(f"SELECT status, COUNT(*) AS rules FROM {TABLE_RULES_STAGING} "
         f"WHERE run_config_name = '{run_config_name}' GROUP BY status ORDER BY status")
else:
    print("  (none yet)")

section("Latest review notifications")
if exists(TABLE_REVIEW_NOTIFICATIONS):
    show(f"SELECT created_at, new_count, pending_total, concat_ws(', ', rule_names) AS rules "
         f"FROM {TABLE_REVIEW_NOTIFICATIONS} WHERE run_config_name = '{run_config_name}' "
         f"ORDER BY created_at DESC LIMIT 5")
else:
    print("  (none yet)")
