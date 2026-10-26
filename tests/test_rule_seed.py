"""Tests for the YAML -> DQX check conversion used to seed dq_rules_registry."""

import os

import pytest
import yaml

from src.rule_seed import yaml_rule_to_dqx_check, yaml_rules_to_dqx_checks

CONFIGS = os.path.join(os.path.dirname(__file__), "..", "configs")


def test_min_value_becomes_is_not_less_than():
    check = yaml_rule_to_dqx_check({
        "name": "q", "column": "quantity", "type": "min_value", "min": 1,
        "criticality": "error", "message": "m",
    })
    assert check["check"] == {
        "function": "is_not_less_than",
        "for_each_column": ["quantity"],
        "arguments": {"limit": 1},
    }


def test_warning_becomes_warn_and_regex_renamed():
    check = yaml_rule_to_dqx_check({
        "name": "d", "column": "transaction_date", "type": "matches_regex",
        "regex": "^x$", "criticality": "warning", "message": "m",
    })
    assert check["criticality"] == "warn"
    assert check["check"]["function"] == "regex_match"


def test_is_in_list_uses_native_list():
    check = yaml_rule_to_dqx_check({
        "name": "a", "column": "division", "type": "is_in_list",
        "allowed_values": ["011", "012"], "criticality": "error", "message": "m",
    })
    assert check["check"]["arguments"] == {"allowed": ["011", "012"]}


def test_custom_sql_becomes_sql_expression_without_columns():
    check = yaml_rule_to_dqx_check({
        "name": "c", "column": "division", "type": "custom_sql",
        "sql_expression": "a = 1", "criticality": "error", "message": "m",
    })
    assert check["check"]["function"] == "sql_expression"
    assert "for_each_column" not in check["check"]


def test_unsupported_type_raises():
    with pytest.raises(ValueError):
        yaml_rule_to_dqx_check({"name": "x", "column": "c", "type": "nope"})


@pytest.mark.parametrize("fname", ["rules_batch.yml", "rules_streaming.yml"])
def test_shipped_yaml_files_all_convert(fname):
    with open(os.path.join(CONFIGS, fname), encoding="utf-8") as f:
        checks = yaml_rules_to_dqx_checks(yaml.safe_load(f))
    assert checks
    assert all(c["criticality"] in ("error", "warn") for c in checks)
