"""
Review service — the single implementation of "approve / reject a pending rule".

Used by two front doors:
  - notebooks/review_rule.py   (the "[RTM] Review Rule" job)
  - live_board/cluster_session (Approve / Reject buttons on the live board, run on
    the cluster because DQX validation needs a Spark session)

`decide` holds the decision logic and is pure (callables are injected), so it is
unit-tested without Spark. `run_review` gathers the data from Spark, calls
`decide`, and returns a JSON-friendly result dict.
"""

from __future__ import annotations

from ai_suggester.rule_writer import promote_rule, reject_rule
from config.constants import TABLE_RULES, TABLE_RULES_STAGING
from src.rule_reload import is_skipped_message
from src.rule_review import (
    TARGET_TABLE_BY_RUN_CONFIG,
    ReviewError,
    check_column,
    resolve_rule,
    staged_to_check_dict,
)

VALID_ACTIONS = ("list", "approve", "reject")
RESERVED_DQX_COLUMNS = ("_errors", "_warnings")


def decide(
    action: str,
    rule_ref: str,
    pending: list[dict],
    target_columns: list[str] | None,
    live_names: set[str],
    *,
    reviewer: str,
    promote,
    reject,
    validate,
    dry_run=None,
) -> dict:
    """
    Apply one review decision. Returns {"ok", "message", "rule"}; a refused decision
    comes back as ok=False with the reason (nothing is written in that case).

    promote(rule_id, reviewer) / reject(rule_id, reviewer) perform the writes;
    validate(list_of_checks) returns an object with .has_errors and .errors.
    dry_run(check) applies the rule to a sample of real events and raises if it cannot be
    applied (unknown column or function, type mismatch...): DQX validation only checks a
    rule's shape, so this is what stops a rule that would crash the stream.
    """
    try:
        if action not in VALID_ACTIONS:
            raise ReviewError(f"Unknown action '{action}'. Use list, approve or reject.")
        if action == "list":
            return {"ok": True, "message": f"{len(pending)} pending suggestion(s)", "rule": None}

        row = resolve_rule(pending, rule_ref)

        if action == "approve":
            if row["name"] in live_names:
                raise ReviewError(f"A rule named '{row['name']}' is already live — reject this duplicate instead.")
            if target_columns is not None:
                check_column(row["column_name"], target_columns)
            status = validate([staged_to_check_dict(row)])
            if status.has_errors:
                raise ReviewError(f"DQX rejected this rule: {status.errors}")
            if dry_run is not None:
                try:
                    dry_run(staged_to_check_dict(row))
                except Exception as exc:                       # noqa: BLE001 - any failure means "do not enforce"
                    reason = str(exc).strip().splitlines()[0][:240] if str(exc).strip() else type(exc).__name__
                    raise ReviewError(f"Trial run on live events failed, so the rule was NOT approved: {reason}")
            promote(row["rule_id"], reviewer)
            message = f"Approved '{row['name']}'. A running stream applies it on its next micro-batch (~30 s)."
        else:
            reject(row["rule_id"], reviewer)
            message = f"Rejected '{row['name']}'. It will never be enforced."
        return {"ok": True, "message": message, "rule": row}
    except ReviewError as exc:
        return {"ok": False, "message": str(exc), "rule": None}


# ---------------------------------------------------------------------------
# Spark-backed wrapper
# ---------------------------------------------------------------------------

def _dqx_validate(checks: list[dict]):
    from databricks.labs.dqx.engine import DQEngine
    from databricks.sdk import WorkspaceClient

    return DQEngine(WorkspaceClient()).validate_checks(checks)


def _make_dry_run(spark, target_table: str | None):
    """
    Build the trial-run callable: apply one check to a few real events with the same engine
    the stream uses. Returns None when there is no table to try it on (nothing to test against).
    """
    if not target_table or not spark.catalog.tableExists(target_table):
        return None

    def dry_run(check: dict) -> None:
        from databricks.labs.dqx.engine import DQEngine
        from databricks.sdk import WorkspaceClient
        from pyspark.sql.functions import expr

        # The clean/quarantine tables already carry DQX's own result columns, which DQX refuses as input.
        sample = spark.table(target_table).drop(*RESERVED_DQX_COLUMNS).limit(5)
        engine = DQEngine(WorkspaceClient())
        result = engine.apply_checks_by_metadata(
            sample, [check],
            custom_check_functions={"sql_expression": lambda df, **kwargs: expr(kwargs["expression"])},
        )
        rows = result.collect()                                # force execution, not just planning
        for row in rows:
            for entry in list(row["_errors"] or []) + list(row["_warnings"] or []):
                if is_skipped_message(entry["message"]):       # DQX swallows it and fails every row instead
                    raise RuntimeError(entry["message"])

    return dry_run


def _pending_rows(spark, run_config_name: str) -> list[dict]:
    if not spark.catalog.tableExists(TABLE_RULES_STAGING):
        return []
    rows = (
        spark.table(TABLE_RULES_STAGING)
        .filter(f"run_config_name = '{run_config_name}' AND status = 'pending'")
        .orderBy("source", "name")
        .collect()
    )
    return [r.asDict() for r in rows]


def _live_names(spark, run_config_name: str) -> set[str]:
    if not spark.catalog.tableExists(TABLE_RULES):
        return set()
    rows = spark.table(TABLE_RULES).filter(f"run_config_name = '{run_config_name}'").select("name").collect()
    return {r["name"] for r in rows}


def _audit_and_live(spark, rule_id: str, name: str, run_config_name: str) -> tuple[dict | None, dict | None]:
    audit = spark.table(TABLE_RULES_STAGING).filter(f"rule_id = '{rule_id}'").first()
    live = spark.table(TABLE_RULES).filter(f"run_config_name = '{run_config_name}' AND name = '{name}'").first()
    return (
        {"name": audit["name"], "status": audit["status"], "reviewed_by": audit["reviewed_by"],
         "reviewed_at": audit["reviewed_at"]} if audit else None,
        {"name": live["name"], "function": live["check"]["function"],
         "arguments": dict(live["check"]["arguments"]), "criticality": live["criticality"]} if live else None,
    )


def run_review(
    spark,
    action: str,
    rule_ref: str = "",
    run_config_name: str = "streaming_event_rules",
    *,
    reviewer: str | None = None,
    promote=None,
    reject=None,
    validate=None,
    dry_run=None,
) -> dict:
    """Run one review action against the real tables and return a JSON-friendly result."""
    reviewer = reviewer or spark.sql("SELECT current_user()").first()[0]
    target = TARGET_TABLE_BY_RUN_CONFIG.get(run_config_name)
    columns = spark.table(target).columns if target and spark.catalog.tableExists(target) else None

    result = decide(
        action,
        rule_ref,
        _pending_rows(spark, run_config_name),
        columns,
        _live_names(spark, run_config_name),
        reviewer=reviewer,
        promote=promote or (lambda rule_id, who: promote_rule(rule_id, who, spark)),
        reject=reject or (lambda rule_id, who: reject_rule(rule_id, who, spark)),
        validate=validate or _dqx_validate,
        dry_run=dry_run if dry_run is not None else _make_dry_run(spark, target),
    )
    result.update({"action": action, "reviewer": reviewer, "audit": None, "live": None})
    if result["ok"] and result["rule"]:
        result["audit"], result["live"] = _audit_and_live(
            spark, result["rule"]["rule_id"], result["rule"]["name"], run_config_name)
    result["pending"] = _pending_rows(spark, run_config_name)
    return result
