"""Tests for the continuous suggestion cycle's pure logic."""

from ai_suggester.suggestion_cycle import (
    HOW_TO_REVIEW,
    build_notification,
    format_notification,
    should_run_cycle,
)
from ai_suggester.rule_writer import filter_new_suggestions


# ---------------------------------------------------------------------------
# should_run_cycle
# ---------------------------------------------------------------------------

def test_first_cycle_waits_for_minimum_rows():
    assert not should_run_cycle(59, None, min_rows=60, min_new_rows=30)
    assert should_run_cycle(60, None, min_rows=60, min_new_rows=30)


def test_later_cycle_needs_enough_new_rows():
    assert not should_run_cycle(80, 60, min_rows=60, min_new_rows=30)
    assert should_run_cycle(90, 60, min_rows=60, min_new_rows=30)


def test_zero_growth_never_runs_after_first_cycle():
    assert not should_run_cycle(100, 100, min_rows=60, min_new_rows=1)


# ---------------------------------------------------------------------------
# notification
# ---------------------------------------------------------------------------

def test_notification_fields():
    n = build_notification("cid", "streaming_event_rules", ["a", "b"], pending_total=7)
    assert n["new_count"] == 2
    assert n["pending_total"] == 7
    assert n["rule_names"] == ["a", "b"]
    assert n["how_to_review"] == HOW_TO_REVIEW


def test_notification_text_names_rules_and_how_to_review():
    text = format_notification(build_notification("cid", "streaming_event_rules", ["a", "b"], 3))
    assert "a, b" in text
    assert "streaming_event_rules" in text
    assert "Review Rule" in text


# ---------------------------------------------------------------------------
# per-cycle cap (filter_new_suggestions max_new)
# ---------------------------------------------------------------------------

def _rule(name):
    return {"name": name, "function": "is_not_null", "constraint": {}}


def test_cap_limits_new_suggestions():
    rules = [_rule(f"r{i}") for i in range(10)]
    assert len(filter_new_suggestions(rules, set(), set(), max_new=5)) == 5


def test_cap_applies_after_deduplication():
    rules = [_rule("live"), _rule("a"), _rule("b"), _rule("c")]
    kept = filter_new_suggestions(rules, set(), {"live"}, max_new=2)
    assert [r["name"] for r in kept] == ["a", "b"]


def test_no_cap_by_default():
    rules = [_rule(f"r{i}") for i in range(10)]
    assert len(filter_new_suggestions(rules, set(), set())) == 10
