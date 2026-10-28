"""Tests for the rule review job's pure logic."""

import pytest

from src.rule_review import (
    ReviewError,
    check_column,
    format_pending,
    resolve_rule,
    staged_to_check_dict,
)


def _row(rule_id, name, function="is_not_null", column="quantity", constraint="{}",
         criticality="error", message="m", source="statistical", confidence=0.9):
    return {"rule_id": rule_id, "name": name, "function": function, "column_name": column,
            "constraint": constraint, "criticality": criticality, "message": message,
            "source": source, "confidence": confidence, "reasoning": "because"}


# ---------------------------------------------------------------------------
# resolve_rule
# ---------------------------------------------------------------------------

def test_resolve_by_name():
    pending = [_row("id1", "a"), _row("id2", "b")]
    assert resolve_rule(pending, "b")["rule_id"] == "id2"


def test_resolve_by_rule_id():
    pending = [_row("id1", "a"), _row("id2", "b")]
    assert resolve_rule(pending, "id1")["name"] == "a"


def test_resolve_unknown_lists_pending_names():
    with pytest.raises(ReviewError, match="Pending: a, b"):
        resolve_rule([_row("id1", "a"), _row("id2", "b")], "zzz")


def test_resolve_ambiguous_name_asks_for_id():
    pending = [_row("id1", "q", constraint='{"max": 1}'), _row("id2", "q", constraint='{"max": 2}')]
    with pytest.raises(ReviewError, match="rule_id"):
        resolve_rule(pending, "q")


def test_resolve_empty_reference():
    with pytest.raises(ReviewError):
        resolve_rule([_row("id1", "a")], "  ")


# ---------------------------------------------------------------------------
# check_column
# ---------------------------------------------------------------------------

def test_existing_column_is_accepted_case_insensitively():
    check_column("Quantity", ["quantity", "unit_price"])


def test_missing_column_is_refused():
    with pytest.raises(ReviewError, match="does not exist"):
        check_column("transaction_id", ["event_id", "quantity"])


def test_no_column_is_accepted():
    check_column(None, ["quantity"])
    check_column("", ["quantity"])


# ---------------------------------------------------------------------------
# staged_to_check_dict
# ---------------------------------------------------------------------------

def test_min_value_translates_to_dqx_with_numeric_limit():
    c = staged_to_check_dict(_row("i", "q", "min_value", "quantity", '{"min": 1}', "error"))
    assert c["check"] == {"function": "is_not_less_than", "arguments": {"limit": 1},
                          "for_each_column": ["quantity"]}
    assert c["criticality"] == "error"


def test_warning_becomes_warn_and_list_is_decoded():
    c = staged_to_check_dict(_row("i", "d", "is_in_list", "division",
                                  '{"allowed": ["011", "012"]}', "warning"))
    assert c["criticality"] == "warn"
    assert c["check"]["arguments"] == {"allowed": ["011", "012"]}


def test_sql_expression_has_no_for_each_column():
    c = staged_to_check_dict(_row("i", "s", "sql_expression", "division",
                                  '{"expression": "quantity > 0"}'))
    assert "for_each_column" not in c["check"]
    assert c["check"]["arguments"] == {"expression": "quantity > 0"}


# ---------------------------------------------------------------------------
# format_pending
# ---------------------------------------------------------------------------

def test_format_pending_empty():
    assert "no pending" in format_pending([])


def test_format_pending_lists_names():
    text = format_pending([_row("i", "quantity_not_null")])
    assert "quantity_not_null" in text
    assert "90%" in text
