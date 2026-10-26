"""
Convert the YAML rule definitions in configs/ into DQX-native check dicts
(the metadata format accepted by DQEngine.save_checks / validate_checks).
"""

from __future__ import annotations

# YAML `type` -> (DQX function, {yaml key: DQX argument name})
_TYPE_MAP = {
    "is_not_null":   ("is_not_null",         {}),
    "is_in_list":    ("is_in_list",          {"allowed_values": "allowed"}),
    "min_value":     ("is_not_less_than",    {"min": "limit"}),
    "max_value":     ("is_not_greater_than", {"max": "limit"}),
    "matches_regex": ("regex_match",         {"regex": "regex"}),
}
_CRITICALITY = {"warning": "warn"}


def yaml_rule_to_dqx_check(rule: dict) -> dict:
    """Translate one entry of a configs/rules_*.yml `rules` list."""
    rule_type = rule["type"]
    if rule_type == "custom_sql":
        function, for_each_column = "sql_expression", None
        arguments = {"expression": rule["sql_expression"]}
    elif rule_type in _TYPE_MAP:
        function, renames = _TYPE_MAP[rule_type]
        for_each_column = [rule["column"]]
        arguments = {dqx_key: rule[yaml_key] for yaml_key, dqx_key in renames.items()}
    else:
        raise ValueError(f"Rule '{rule['name']}' has unsupported type '{rule_type}'")

    criticality = rule.get("criticality", "error")
    check = {"function": function, "arguments": arguments}
    if for_each_column:  # DQX rejects an empty for_each_column
        check["for_each_column"] = for_each_column
    return {
        "name": rule["name"],
        "criticality": _CRITICALITY.get(criticality, criticality),
        "check": check,
        "user_metadata": {"message": rule["message"]} if rule.get("message") else {},
    }


def yaml_rules_to_dqx_checks(rule_config: dict) -> list[dict]:
    return [yaml_rule_to_dqx_check(r) for r in rule_config["rules"]]
