"""
Read-only status snapshot of the streaming loop, shared by the one-shot
"[RTM] Live Status" job and the live_monitor task inside the streaming pipeline.
"""

from __future__ import annotations

from config.constants import (
    TABLE_BATCH_METRICS,
    TABLE_REVIEW_NOTIFICATIONS,
    TABLE_RULE_METRICS,
    TABLE_RULES,
    TABLE_RULES_STAGING,
    TABLE_STREAM_CLEAN,
    TABLE_STREAM_QUARANTINE,
)


def format_table(columns: list[str], rows: list[list], empty_msg: str = "  (nothing yet)") -> str:
    """Render rows as a simple ' | ' separated table (header + one line per row)."""
    if not rows:
        return empty_msg
    lines = ["  " + " | ".join(columns)]
    lines += ["  " + " | ".join(str(v) for v in row) for row in rows]
    return "\n".join(lines)


def _section(title: str) -> None:
    print(f"\n=== {title} ===")


def _show(spark, sql: str, empty_msg: str = "  (nothing yet)") -> None:
    rows = spark.sql(sql).collect()
    if not rows:
        print(empty_msg)
        return
    cols = list(rows[0].__fields__)
    print(format_table(cols, [[r[c] for c in cols] for r in rows]))


def render_snapshot(spark, run_config_name: str = "streaming_event_rules", last_batches: int = 5) -> None:
    """Print the current state of the streaming loop (changes nothing)."""

    def exists(table: str) -> bool:
        return spark.catalog.tableExists(table)

    _section("Event flow")
    for label, table in (("clean", TABLE_STREAM_CLEAN), ("quarantine", TABLE_STREAM_QUARANTINE)):
        print(f"  {label:<11}: {spark.table(table).count() if exists(table) else 0} rows")
    if exists(TABLE_BATCH_METRICS):
        print(f"\n  last {last_batches} micro-batches:")
        _show(spark,
              f"SELECT batch_id, clean_count, quarantine_count, total_count, processed_at "
              f"FROM {TABLE_BATCH_METRICS} ORDER BY batch_id DESC LIMIT {last_batches}")

    _section(f"Active rules ({run_config_name})")
    if exists(TABLE_RULES):
        _show(spark,
              f"SELECT name, `check`.function AS function, criticality, created_at FROM {TABLE_RULES} "
              f"WHERE run_config_name = '{run_config_name}' ORDER BY created_at, name")
    else:
        print("  (registry not created yet)")

    _section("Rules firing in the stream (first seen = when a rule first caught an event)")
    if exists(TABLE_RULE_METRICS):
        _show(spark,
              f"SELECT rule_name, MIN(recorded_at) AS first_seen, COUNT(*) AS batches_fired, "
              f"MAX_BY(violation_pct, recorded_at) AS last_batch_pct "
              f"FROM {TABLE_RULE_METRICS} WHERE run_config_name = '{run_config_name}' AND run_type = 'streaming' "
              f"GROUP BY rule_name ORDER BY first_seen")
    else:
        print("  (no metrics yet)")

    _section("Suggestions waiting for review")
    if exists(TABLE_RULES_STAGING):
        _show(spark,
              f"SELECT name, function, criticality, source, confidence FROM {TABLE_RULES_STAGING} "
              f"WHERE run_config_name = '{run_config_name}' AND status = 'pending' ORDER BY source, name")
        _show(spark,
              f"SELECT status, COUNT(*) AS rules FROM {TABLE_RULES_STAGING} "
              f"WHERE run_config_name = '{run_config_name}' GROUP BY status ORDER BY status")
    else:
        print("  (none yet)")

    _section("Latest review notifications")
    if exists(TABLE_REVIEW_NOTIFICATIONS):
        _show(spark,
              f"SELECT created_at, new_count, pending_total, concat_ws(', ', rule_names) AS rules "
              f"FROM {TABLE_REVIEW_NOTIFICATIONS} WHERE run_config_name = '{run_config_name}' "
              f"ORDER BY created_at DESC LIMIT 5")
    else:
        print("  (none yet)")
