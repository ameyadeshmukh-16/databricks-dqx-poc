"""
Unit tests for ai_suggester.rule_generator.

No Spark required — the generator operates purely on a profile dict.
Run with: pytest tests/test_rule_generator.py -v
"""

import pytest

from ai_suggester.rule_generator import suggest_statistical_rules, _round_up_ceiling
from config.constants import STAT_DISTINCT_THRESHOLD


RUN_CONFIG = "batch_sales_rules"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _profile(col_name, dtype="StringType", null_pct=0.0, distinct_count=3,
             distinct_values=None, col_min=None, col_max=None, mean=None, stddev=None):
    entry = {
        "dtype": dtype,
        "null_pct": null_pct,
        "distinct_count": distinct_count,
        "sample_values": [],
    }
    if distinct_values is not None:
        entry["distinct_values"] = distinct_values
    if col_min is not None:
        entry["min"] = col_min
        entry["max"] = col_max
        entry["mean"] = mean or 0.0
        entry["stddev"] = stddev or 0.0
    return {col_name: entry}


def _rule_names(rules):
    return {r["name"] for r in rules}


# ---------------------------------------------------------------------------
# is_not_null
# ---------------------------------------------------------------------------

def test_null_rule_generated_for_zero_null_pct():
    rules = suggest_statistical_rules(_profile("event_id", null_pct=0.0), RUN_CONFIG)
    assert "event_id_not_null" in _rule_names(rules)


def test_null_rule_generated_when_null_pct_just_below_threshold():
    rules = suggest_statistical_rules(_profile("event_id", null_pct=0.009), RUN_CONFIG)
    assert "event_id_not_null" in _rule_names(rules)


def test_no_null_rule_when_null_pct_at_threshold():
    rules = suggest_statistical_rules(_profile("event_id", null_pct=0.01), RUN_CONFIG)
    assert "event_id_not_null" not in _rule_names(rules)


def test_no_null_rule_when_null_pct_high():
    rules = suggest_statistical_rules(_profile("event_id", null_pct=0.50), RUN_CONFIG)
    assert "event_id_not_null" not in _rule_names(rules)


def test_null_rule_criticality_is_error():
    rules = suggest_statistical_rules(_profile("event_id", null_pct=0.0), RUN_CONFIG)
    null_rule = next(r for r in rules if r["name"] == "event_id_not_null")
    assert null_rule["criticality"] == "error"


# ---------------------------------------------------------------------------
# is_in_list / sql_expression allowlist
# ---------------------------------------------------------------------------

def test_allowlist_rule_for_low_cardinality():
    p = _profile(
        "division",
        null_pct=0.0,
        distinct_count=3,
        distinct_values=["011", "012", "013"],
    )
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "division_valid_values" in _rule_names(rules)


def test_no_allowlist_when_distinct_values_not_collected():
    # distinct_values key absent → high cardinality, threshold exceeded
    p = _profile("free_text", null_pct=0.0, distinct_count=STAT_DISTINCT_THRESHOLD + 1)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "free_text_valid_values" not in _rule_names(rules)


def test_no_allowlist_for_numeric_column():
    p = _profile(
        "quantity",
        dtype="IntegerType",
        null_pct=0.0,
        distinct_count=5,
        distinct_values=["1", "2", "3", "4", "5"],
        col_min=1, col_max=5, mean=3.0, stddev=1.2,
    )
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "quantity_valid_values" not in _rule_names(rules)


def test_no_allowlist_when_coverage_too_low():
    # null_pct=0.10 → 90% non-null coverage, below STAT_COVERAGE_THRESHOLD (0.95)
    p = _profile(
        "division",
        null_pct=0.10,
        distinct_count=3,
        distinct_values=["011", "012", "013"],
    )
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "division_valid_values" not in _rule_names(rules)


# ---------------------------------------------------------------------------
# min_value / max_value
# ---------------------------------------------------------------------------

def test_min_value_rule_generated_when_always_positive():
    p = _profile("quantity", dtype="IntegerType", null_pct=0.0,
                 distinct_count=10, col_min=1, col_max=100, mean=25.0)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "quantity_min_value" in _rule_names(rules)


def test_no_min_value_rule_when_min_is_zero():
    p = _profile("quantity", dtype="IntegerType", null_pct=0.0,
                 distinct_count=10, col_min=0, col_max=100, mean=25.0)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "quantity_min_value" not in _rule_names(rules)


def test_no_min_value_rule_when_min_is_negative():
    p = _profile("temperature", dtype="DoubleType", null_pct=0.0,
                 distinct_count=100, col_min=-5.0, col_max=100.0, mean=20.0)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "temperature_min_value" not in _rule_names(rules)


def test_max_value_rule_generated():
    p = _profile("price", dtype="DoubleType", null_pct=0.0,
                 distinct_count=50, col_min=0.99, col_max=299.99, mean=45.0)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    assert "price_max_value" in _rule_names(rules)


# ---------------------------------------------------------------------------
# Source and metadata
# ---------------------------------------------------------------------------

def test_all_rules_have_source_statistical():
    p = _profile("event_id", null_pct=0.0)
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    for r in rules:
        assert r["source"] == "statistical"


def test_all_rules_have_required_keys():
    p = _profile("event_id", null_pct=0.0, distinct_count=3,
                 distinct_values=["A", "B", "C"])
    rules = suggest_statistical_rules(p, RUN_CONFIG)
    required = {"name", "column", "function", "constraint", "criticality",
                "message", "source", "confidence", "reasoning"}
    for r in rules:
        assert required.issubset(r.keys()), f"Rule missing keys: {required - r.keys()}"


# ---------------------------------------------------------------------------
# _round_up_ceiling helper
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    (1,      10),
    (9,      90),
    (10,     100),
    (11,     200),
    (99,     1000),
    (100,    1000),
    (299.99, 3000),
    (1000,   10000),
])
def test_round_up_ceiling(value, expected):
    assert _round_up_ceiling(value) == expected


# ---------------------------------------------------------------------------
# code-like integer columns
# ---------------------------------------------------------------------------

def test_no_range_rules_for_code_like_integer_column():
    profile = {"division": {
        "dtype": "IntegerType", "null_pct": 0.0, "distinct_count": 3,
        "min": 11.0, "max": 999.0, "mean": 100.0, "stddev": 1.0,
    }}
    names = _rule_names(suggest_statistical_rules(profile, "cfg"))
    assert "division_min_value" not in names
    assert "division_max_value" not in names


def test_range_rules_still_generated_for_plain_integer_column():
    profile = {"quantity": {
        "dtype": "IntegerType", "null_pct": 0.0, "distinct_count": 10,
        "min": 1.0, "max": 10.0, "mean": 5.0, "stddev": 1.0,
    }}
    names = _rule_names(suggest_statistical_rules(profile, "cfg"))
    assert "quantity_min_value" in names
    assert "quantity_max_value" in names
