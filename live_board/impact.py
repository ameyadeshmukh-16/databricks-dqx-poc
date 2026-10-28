"""
Impact preview: "what would this suggested rule have done to recent events?"

Turns a staged rule into a SQL condition that is TRUE for an event that would
violate it, and measures it over the most recent events (clean + quarantined).
The rule text comes from an LLM, so nothing is executed unless it passes strict
checks: identifiers must be real event columns, literals are escaped, and a
free-form SQL expression must pass a token allowlist (no statements, comments,
sub-queries or unknown functions). Anything else is reported as unavailable.
A preview is read-only and never replaces the DQX validation done on approve.
"""

from __future__ import annotations

import json
import math
import re
from typing import Iterable

from config.constants import TABLE_STREAM_CLEAN, TABLE_STREAM_QUARANTINE

# Keep in sync with event_schema in notebooks/kafka_stream_dqx_validation.py (+ ingested_at).
EVENT_COLUMNS = (
    "event_id", "store_id", "product_id", "division", "quantity",
    "unit_price", "coupon_applied", "event_timestamp", "ingested_at",
)
RECENT_EVENTS = 2000
MAX_LIST_VALUES = 50
MAX_TEXT = 300

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_CHARS = re.compile(r"^[A-Za-z0-9_\s()<>=!+\-*/%.,'\"]+$")
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_KEYWORDS = {"and", "or", "not", "in", "is", "null", "like", "rlike", "between", "true", "false", "as",
             "case", "when", "then", "else", "end"}
_TYPES = {"string", "int", "integer", "bigint", "double", "float", "boolean", "timestamp", "date", "decimal"}
_FUNCTIONS = {"lower", "upper", "length", "abs", "trim", "coalesce", "cast", "try_cast", "to_timestamp",
              "try_to_timestamp", "current_timestamp", "current_date", "round", "concat", "nvl"}


# ---------------------------------------------------------------------------
# Safe building blocks
# ---------------------------------------------------------------------------

def sql_string(value) -> str:
    """A single-quoted Spark SQL string literal with backslashes and quotes escaped."""
    text = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{text}'"


def _number(value) -> str:
    number = float(value)                      # ValueError / TypeError for junk
    if math.isnan(number) or math.isinf(number):
        raise ValueError("not a finite number")
    return repr(int(number)) if number.is_integer() else repr(number)


def _literal(value) -> str:
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return _number(value)
    return sql_string(value)


def resolve_column(name: str | None, columns: Iterable[str]) -> str | None:
    """The real column name (matched case-insensitively), or None if it is not a valid event column."""
    if not name or not _IDENT.match(name):
        return None
    by_lower = {c.lower(): c for c in columns}
    return by_lower.get(name.lower())


def safe_expression(expression: str, columns: Iterable[str]) -> bool:
    """True only for a plain boolean expression over known columns and allowlisted functions."""
    if not expression or len(expression) > MAX_TEXT:
        return False
    if ";" in expression or "--" in expression or "/*" in expression or "*/" in expression:
        return False

    # String literals are just data: swap each for a number, then no quote may remain (unbalanced quote).
    bare = _STRING_LITERAL.sub(" 0 ", expression)
    if "'" in bare or '"' in bare:
        return False
    if not _ALLOWED_CHARS.match(bare):
        return False

    known = {c.lower() for c in columns}
    for match in _WORD.finditer(bare):
        word = match.group(0).lower()
        if word in _KEYWORDS or word in _TYPES:
            continue                                       # IN (...), AND (...), CAST(x AS STRING) ...
        if bare[match.end():].lstrip().startswith("("):
            if word not in _FUNCTIONS:
                return False                               # unknown function call
        elif word not in known:
            return False                                   # unknown identifier
    return True


# ---------------------------------------------------------------------------
# Rule -> "event violates it" condition
# ---------------------------------------------------------------------------

def _constraint(row: dict) -> dict:
    raw = row.get("constraint") or "{}"
    try:
        value = json.loads(raw) if isinstance(raw, str) else dict(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def violation_predicate(row: dict, columns: Iterable[str] = EVENT_COLUMNS) -> str | None:
    """SQL condition that is TRUE when an event breaks the staged rule, or None if it cannot be tested safely."""
    function = row.get("function")
    c = _constraint(row)
    columns = list(columns)
    try:
        if function == "sql_expression":
            expression = str(c.get("expression", ""))
            return f"NOT ({expression})" if safe_expression(expression, columns) else None

        col = resolve_column(row.get("column_name"), columns)
        if col is None:
            return None
        if function == "is_not_null":
            return f"{col} IS NULL"
        if function == "min_value":
            return f"{col} < {_number(c['min'])}"
        if function == "max_value":
            return f"{col} > {_number(c['max'])}"
        if function == "is_in_list":
            allowed = list(c.get("allowed", []))
            if not allowed or len(allowed) > MAX_LIST_VALUES:
                return None
            return f"NOT ({col} IN ({', '.join(_literal(v) for v in allowed)}))"
        if function == "matches_regex":
            pattern = str(c.get("regex") or c.get("pattern") or "")
            if not pattern or len(pattern) > MAX_TEXT:
                return None
            return f"NOT (CAST({col} AS STRING) RLIKE {sql_string(pattern)})"
    except (KeyError, TypeError, ValueError):
        return None
    return None


def impact_query(predicate: str, recent: int = RECENT_EVENTS) -> str:
    """Count recent events, those the rule would flag, and those among them that pass today."""
    cols = ", ".join(EVENT_COLUMNS)
    return (
        f"WITH recent AS ("
        f"SELECT {cols}, 'clean' AS src FROM {TABLE_STREAM_CLEAN} "
        f"UNION ALL "
        f"SELECT {cols}, 'quarantine' AS src FROM {TABLE_STREAM_QUARANTINE} "
        f"ORDER BY ingested_at DESC LIMIT {int(recent)}) "
        f"SELECT COUNT(*) AS total, "
        f"COUNT_IF({predicate}) AS hits, "
        f"COUNT_IF(({predicate}) AND src = 'clean') AS new_hits "
        f"FROM recent"
    )


def preview_rule(runner, row: dict) -> dict:
    """Measure one staged rule. Never raises: failures come back as a status the UI can show."""
    predicate = violation_predicate(row)
    if predicate is None:
        return {"status": "unavailable"}
    try:
        res = runner.query(impact_query(predicate))[0]
    except Exception as exc:                       # noqa: BLE001 - e.g. a type mismatch in an LLM-written rule
        return {"status": "error", "detail": str(exc).splitlines()[0][:160]}
    total, hits, new_hits = int(res["total"]), int(res["hits"]), int(res["new_hits"])
    return {
        "status": "ok",
        "total": total,
        "hits": hits,
        "new_hits": new_hits,
        "hit_pct": 100.0 * hits / total if total else 0.0,
        "new_pct": 100.0 * new_hits / total if total else 0.0,
    }


def describe_impact(result: dict | None) -> str:
    """One line for the review card."""
    if not result:
        return "Impact preview: calculating…"
    if result["status"] == "unavailable":
        return "Impact preview not available for this rule type."
    if result["status"] == "error":
        return "Impact preview could not be computed for this rule."
    if result["total"] == 0:
        return "Impact preview: no recent events to test against."
    text = f"Would have flagged {result['hit_pct']:.1f}% of the last {result['total']:,} events"
    if result["new_hits"]:
        return text + f" — {result['new_pct']:.1f}% would be newly caught (they pass today)."
    return text + " — nothing new: those events are already caught by existing rules." if result["hits"] \
        else text + "."
