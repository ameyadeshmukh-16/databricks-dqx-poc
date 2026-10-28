"""Tests for the stream hot-reload rule diff."""

from src.rule_reload import rule_set_changes


def _rule(name, function="is_not_null", criticality="error", **args):
    return {"name": name, "criticality": criticality,
            "check": {"function": function, "arguments": args}}


def test_no_changes():
    rules = [_rule("a"), _rule("b")]
    assert rule_set_changes(rules, list(rules)) == ([], [], [])


def test_added_rule_is_reported():
    old = [_rule("a")]
    new = [_rule("a"), _rule("b")]
    assert rule_set_changes(old, new) == (["b"], [], [])


def test_removed_rule_is_reported():
    assert rule_set_changes([_rule("a"), _rule("b")], [_rule("a")]) == ([], ["b"], [])


def test_changed_arguments_are_reported():
    old = [_rule("q", "is_not_less_than", limit="1")]
    new = [_rule("q", "is_not_less_than", limit="5")]
    assert rule_set_changes(old, new) == ([], [], ["q"])


def test_changed_criticality_is_reported():
    assert rule_set_changes([_rule("a")], [_rule("a", criticality="warn")]) == ([], [], ["a"])


def test_results_are_sorted():
    added, _, _ = rule_set_changes([], [_rule("z"), _rule("m"), _rule("a")])
    assert added == ["a", "m", "z"]
