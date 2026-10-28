"""
Pure logic for the rule review job (approve / reject a pending suggestion).

Kept free of Spark so it can be unit-tested; notebooks/review_rule.py wires it
to the staging table and to promote_rule / reject_rule.
"""

from __future__ import annotations

import json

from ai_suggester.rule_writer import to_dqx_check
from config.constants import (
    BATCH_RULE_CONFIG_NAME,
    STREAMING_RULE_CONFIG_NAME,
    TABLE_SALES_CLEAN,
    TABLE_STREAM_CLEAN,
)

# Table whose columns a rule in each group may reference (guards against approving
# a rule for a column the data does not have, which would break the pipeline).
TARGET_TABLE_BY_RUN_CONFIG = {
    STREAMING_RULE_CONFIG_NAME: TABLE_STREAM_CLEAN,
    BATCH_RULE_CONFIG_NAME: TABLE_SALES_CLEAN,
}


class ReviewError(Exception):
    """A review action was refused; the message says why."""


def resolve_rule(pending: list[dict], ref: str) -> dict:
    """
    Pick exactly one pending suggestion by rule_id or by rule name.
    Raises ReviewError if nothing matches or a name matches several rows.
    """
    ref = (ref or "").strip()
    if not ref:
        raise ReviewError("No rule given. Pass the rule name (or rule_id) in the 'rule' parameter.")

    by_id = [r for r in pending if r["rule_id"] == ref]
    if by_id:
        return by_id[0]

    by_name = [r for r in pending if r["name"] == ref]
    if not by_name:
        names = ", ".join(sorted({r["name"] for r in pending})) or "(none)"
        raise ReviewError(f"No pending suggestion named or identified '{ref}'. Pending: {names}")
    if len(by_name) > 1:
        ids = "; ".join(f"{r['rule_id']} ({r['function']} {r['constraint']})" for r in by_name)
        raise ReviewError(f"'{ref}' matches {len(by_name)} pending suggestions — use the rule_id instead: {ids}")
    return by_name[0]


def check_column(column_name: str | None, table_columns: list[str]) -> None:
    """Refuse a rule that references a column the target table does not have."""
    if not column_name:
        return  # cross-column / SQL-expression rules name no single column
    if column_name.lower() not in {c.lower() for c in table_columns}:
        raise ReviewError(
            f"Column '{column_name}' does not exist in the target table "
            f"(columns: {', '.join(table_columns)}) — approving would break validation."
        )


def staged_to_check_dict(row: dict) -> dict:
    """
    Build the DQX check metadata a staged suggestion would become in the registry,
    for DQEngine.validate_checks (same translation promote_rule applies).
    """
    constraint = json.loads(row.get("constraint") or "{}")
    function, arguments, criticality = to_dqx_check(row["function"], constraint, row["criticality"])

    decoded = {}
    for key, value in arguments.items():
        try:
            decoded[key] = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            decoded[key] = value

    check = {"function": function, "arguments": decoded}
    if function != "sql_expression" and row.get("column_name"):
        check["for_each_column"] = [row["column_name"]]

    return {
        "name": row["name"],
        "criticality": criticality,
        "check": check,
        "user_metadata": {"message": row["message"]} if row.get("message") else {},
    }


def format_pending(rows: list[dict]) -> str:
    """Readable table of pending suggestions for the job output."""
    if not rows:
        return "  (no pending suggestions)"
    lines = []
    for r in rows:
        conf = f"{r['confidence']:.0%}" if r.get("confidence") is not None else "-"
        lines.append(
            f"  - {r['name']}  [{r['function']}, {r['criticality']}, source={r['source']}, confidence={conf}]\n"
            f"      constraint: {r['constraint']}\n"
            f"      why: {r.get('reasoning') or r.get('message') or ''}"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Plain-English descriptions (used by the live board's review queue)
# ---------------------------------------------------------------------------

def _constraint(row: dict) -> dict:
    raw = row.get("constraint") or "{}"
    try:
        value = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def describe_rule(row: dict) -> str:
    """One sentence saying what the rule does to an event, for non-technical readers."""
    c = _constraint(row)
    col = row.get("column_name") or "the event"
    function = row.get("function")

    if function == "is_not_null":
        return f"Reject events where {col} is missing"
    if function == "min_value":
        return f"Reject events where {col} is below {c.get('min', '?')}"
    if function == "max_value":
        return f"Reject events where {col} is above {c.get('max', '?')}"
    if function == "is_in_list":
        allowed = c.get("allowed", [])
        shown = ", ".join(str(v) for v in allowed[:6]) + (", …" if len(allowed) > 6 else "")
        return f"Reject events where {col} is not one of: {shown}"
    if function == "matches_regex":
        pattern = c.get("regex") or c.get("pattern") or "?"
        return f"Reject events where {col} does not match the pattern {pattern}"
    if function == "sql_expression":
        return f"Reject events that fail the condition: {c.get('expression', '?')}"
    return f"Apply the '{function}' check to {col}"


def describe_severity(criticality: str | None) -> str:
    """What happens to an event that breaks the rule."""
    if (criticality or "").lower() in ("warn", "warning"):
        return "Warning: the event is flagged but still kept."
    return "Error: the event is sent to quarantine."
