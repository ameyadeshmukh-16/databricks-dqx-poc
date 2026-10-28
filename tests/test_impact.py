"""Tests for the impact preview: predicate building, the safety allowlist, and result handling."""

import json

import pytest

from live_board.impact import (
    EVENT_COLUMNS,
    describe_impact,
    impact_query,
    preview_rule,
    resolve_column,
    safe_expression,
    sql_string,
    violation_predicate,
)


def _rule(function, column="quantity", constraint=None):
    return {"function": function, "column_name": column, "constraint": json.dumps(constraint or {})}


# ---------------------------------------------------------------------------
# predicates per rule type
# ---------------------------------------------------------------------------

def test_not_null_min_max():
    assert violation_predicate(_rule("is_not_null", "store_id")) == "store_id IS NULL"
    assert violation_predicate(_rule("min_value", constraint={"min": 1})) == "quantity < 1"
    assert violation_predicate(_rule("max_value", "unit_price", {"max": 5000.5})) == "unit_price > 5000.5"


def test_in_list_quotes_strings_and_leaves_numbers():
    pred = violation_predicate(_rule("is_in_list", "division", {"allowed": ["011", "012", 13]}))
    assert pred == "NOT (division IN ('011', '012', 13))"


def test_regex_is_escaped_and_cast_to_string():
    pred = violation_predicate(_rule("matches_regex", "event_id", {"regex": "^EVT\\d+$"}))
    assert pred == "NOT (CAST(event_id AS STRING) RLIKE '^EVT\\\\d+$')"


def test_regex_accepts_the_llm_alternative_key():
    assert violation_predicate(_rule("matches_regex", "event_id", {"pattern": "^a$"})) is not None


def test_sql_expression_is_a_pass_condition_so_the_violation_is_its_negation():
    pred = violation_predicate(_rule("sql_expression", "", {"expression": "quantity * unit_price <= 10000"}))
    assert pred == "NOT (quantity * unit_price <= 10000)"


def test_column_matching_is_case_insensitive_and_uses_the_real_name():
    assert violation_predicate(_rule("is_not_null", "Event_ID")) == "event_id IS NULL"


# ---------------------------------------------------------------------------
# things that must NOT be turned into SQL
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("column", ["transaction_id", "quantity; DROP TABLE x", "quantity OR 1=1", "", None, "a b"])
def test_unknown_or_malicious_column_names_are_refused(column):
    assert violation_predicate(_rule("is_not_null", column)) is None
    assert resolve_column(column, EVENT_COLUMNS) is None


@pytest.mark.parametrize("expression", [
    "quantity > 0; DROP TABLE dq_rules_registry",
    "quantity > 0 -- comment",
    "quantity > 0 /* x */",
    "(SELECT COUNT(*) FROM other) > 0",
    "reflect('java.lang.Runtime', 'exec', 'x') = 1",
    "java_method('a', 'b') = 1",
    "unknown_column > 0",
    "quantity > 0 UNION ALL SELECT 1",
    "quantity > `x`",
    "x = 'a' || 'b'",
    "",
])
def test_unsafe_expressions_are_refused(expression):
    assert safe_expression(expression, EVENT_COLUMNS) is False
    assert violation_predicate(_rule("sql_expression", "", {"expression": expression})) is None


@pytest.mark.parametrize("expression", [
    "quantity > 0",
    "unit_price BETWEEN 1 AND 5000",
    "division IN ('011', '012')",
    "event_id IS NOT NULL AND length(event_id) > 3",
    "to_timestamp(event_timestamp) <= current_timestamp()",
    "coalesce(coupon_applied, false) = false",
    "CAST(quantity AS STRING) RLIKE '^[1-9]'",
])
def test_ordinary_expressions_are_accepted(expression):
    assert safe_expression(expression, EVENT_COLUMNS) is True


def test_words_inside_string_literals_do_not_matter():
    assert safe_expression("division <> 'select from drop; table'", EVENT_COLUMNS) is False   # ';' still refused
    assert safe_expression("division <> 'select from drop table'", EVENT_COLUMNS) is True


def test_bad_numbers_and_empty_lists_are_refused():
    assert violation_predicate(_rule("min_value", constraint={"min": "abc"})) is None
    assert violation_predicate(_rule("min_value", constraint={})) is None
    assert violation_predicate(_rule("min_value", constraint={"min": float("nan")})) is None
    assert violation_predicate(_rule("is_in_list", "division", {"allowed": []})) is None
    assert violation_predicate(_rule("is_in_list", "division", {"allowed": list(range(200))})) is None


def test_unsupported_function_and_bad_json():
    assert violation_predicate(_rule("mystery")) is None
    assert violation_predicate({"function": "min_value", "column_name": "quantity", "constraint": "not json"}) is None


def test_string_escaping():
    assert sql_string("it's") == "'it\\'s'"
    assert sql_string("a\\b") == "'a\\\\b'"


# ---------------------------------------------------------------------------
# query + results
# ---------------------------------------------------------------------------

def test_query_reads_recent_events_from_both_tables():
    q = impact_query("quantity < 1", recent=500)
    assert "stream_events_clean" in q and "stream_events_quarantine" in q
    assert "LIMIT 500" in q and "COUNT_IF(quantity < 1)" in q and "src = 'clean'" in q


class FakeRunner:
    def __init__(self, rows=None, error=None):
        self.rows, self.error, self.calls = rows, error, []

    def query(self, sql):
        self.calls.append(sql)
        if self.error:
            raise self.error
        return self.rows


def test_preview_ok_computes_percentages():
    out = preview_rule(FakeRunner([{"total": 2000, "hits": 140, "new_hits": 20}]), _rule("min_value", constraint={"min": 1}))
    assert out["status"] == "ok" and out["hit_pct"] == 7.0 and out["new_pct"] == 1.0


def test_preview_does_not_query_for_unsafe_rules():
    runner = FakeRunner([])
    assert preview_rule(runner, _rule("sql_expression", "", {"expression": "1=1; DROP TABLE x"})) == {"status": "unavailable"}
    assert runner.calls == []


def test_preview_swallows_query_errors():
    out = preview_rule(FakeRunner(error=RuntimeError("type mismatch\nstack...")), _rule("is_not_null"))
    assert out["status"] == "error" and out["detail"] == "type mismatch"


def test_preview_with_no_events():
    out = preview_rule(FakeRunner([{"total": 0, "hits": 0, "new_hits": 0}]), _rule("is_not_null"))
    assert out["hit_pct"] == 0.0


def test_descriptions():
    assert "calculating" in describe_impact(None)
    assert "not available" in describe_impact({"status": "unavailable"})
    assert "could not be computed" in describe_impact({"status": "error"})
    assert "no recent events" in describe_impact({"status": "ok", "total": 0, "hits": 0, "new_hits": 0,
                                                 "hit_pct": 0, "new_pct": 0})
    new = describe_impact({"status": "ok", "total": 2000, "hits": 140, "new_hits": 20, "hit_pct": 7.0, "new_pct": 1.0})
    assert "7.0% of the last 2,000" in new and "1.0% would be newly caught" in new
    old = describe_impact({"status": "ok", "total": 2000, "hits": 140, "new_hits": 0, "hit_pct": 7.0, "new_pct": 0})
    assert "already caught" in old
    none = describe_impact({"status": "ok", "total": 2000, "hits": 0, "new_hits": 0, "hit_pct": 0, "new_pct": 0})
    assert none.endswith("events.")
