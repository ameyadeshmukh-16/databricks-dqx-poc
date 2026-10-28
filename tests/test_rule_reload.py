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


# ---------------------------------------------------------------------------
# fallback helpers
# ---------------------------------------------------------------------------

from src.rule_reload import fallback_culprits, without_blocked


def test_culprits_are_the_added_and_changed_rules():
    last_good = [_rule("a"), _rule("b", "is_not_less_than", limit="1")]
    current = [_rule("a"), _rule("b", "is_not_less_than", limit="9"), _rule("c")]
    assert fallback_culprits(last_good, current) == {"b", "c"}


def test_no_culprits_when_nothing_changed():
    rules = [_rule("a"), _rule("b")]
    assert fallback_culprits(rules, list(rules)) == set()


def test_removed_rules_are_not_blamed():
    assert fallback_culprits([_rule("a"), _rule("b")], [_rule("a")]) == set()


def test_blocked_rules_are_dropped_and_others_kept():
    rules = [_rule("a"), _rule("bad"), _rule("c")]
    assert [r["name"] for r in without_blocked(rules, {"bad"})] == ["a", "c"]
    assert without_blocked(rules, set()) == rules


from src.rule_reload import is_skipped_message


def test_dqx_skipped_check_messages_are_recognised():
    assert is_skipped_message("Check evaluation skipped due to invalid sql expression: 'revenue_total > 0'")
    assert is_skipped_message("INVALID SQL EXPRESSION in check")


def test_ordinary_violation_messages_are_not_mistaken_for_skipped_checks():
    assert not is_skipped_message("Value is less than limit: 1")
    assert not is_skipped_message("Column 'quantity' value is null")
    assert not is_skipped_message(None) and not is_skipped_message("")
