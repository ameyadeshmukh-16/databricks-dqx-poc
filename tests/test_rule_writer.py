"""Tests for the staging -> DQX registry translation in rule_writer."""

from ai_suggester.rule_writer import to_dqx_check


def test_min_value_maps_to_is_not_less_than():
    assert to_dqx_check("min_value", {"min": 0.01}, "error") == (
        "is_not_less_than", {"limit": "0.01"}, "error",
    )


def test_max_value_maps_to_is_not_greater_than():
    fn, args, _ = to_dqx_check("max_value", {"max": 1000}, "error")
    assert fn == "is_not_greater_than"
    assert args == {"limit": "1000"}


def test_matches_regex_maps_to_regex_match():
    fn, args, _ = to_dqx_check("matches_regex", {"regex": "^a$"}, "error")
    assert fn == "regex_match"
    assert args == {"regex": "^a$"}


def test_warning_criticality_maps_to_warn():
    assert to_dqx_check("is_not_null", {}, "warning")[2] == "warn"


def test_list_argument_is_json_encoded():
    _, args, _ = to_dqx_check("is_in_list", {"allowed": ["011", "012"]}, "error")
    assert args == {"allowed": '["011", "012"]'}


def test_unknown_function_passes_through():
    assert to_dqx_check("sql_expression", {"expression": "a > 1"}, "error") == (
        "sql_expression", {"expression": "a > 1"}, "error",
    )


# ---------------------------------------------------------------------------
# filter_new_suggestions
# ---------------------------------------------------------------------------

from ai_suggester.rule_writer import filter_new_suggestions, _suggestion_key


def _rule(name, function="is_not_null", constraint=None):
    return {"name": name, "function": function, "constraint": constraint or {}}


def test_exact_pending_duplicate_is_dropped():
    pending = {_suggestion_key("a_not_null", "is_not_null", '{}')}
    assert filter_new_suggestions([_rule("a_not_null")], pending, set()) == []


def test_same_name_different_constraint_is_kept():
    pending = {_suggestion_key("q_max", "max_value", '{"max": 100}')}
    rules = [_rule("q_max", "max_value", {"max": 200})]
    assert filter_new_suggestions(rules, pending, set()) == rules


def test_constraint_key_order_does_not_matter():
    pending = {_suggestion_key("r", "f", '{"a": 1, "b": 2}')}
    assert filter_new_suggestions([_rule("r", "f", {"b": 2, "a": 1})], pending, set()) == []


def test_live_rule_name_is_dropped():
    assert filter_new_suggestions([_rule("customer_id_not_null")], set(), {"customer_id_not_null"}) == []


def test_duplicates_within_one_batch_are_collapsed():
    rules = [_rule("x"), _rule("x"), _rule("y")]
    assert [r["name"] for r in filter_new_suggestions(rules, set(), set())] == ["x", "y"]
