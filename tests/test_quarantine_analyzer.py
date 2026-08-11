"""
Unit tests for ai_suggester/quarantine_analyzer.py

All tests are pure Python — no Spark required.  The _build_prompt and
_parse_response functions are imported directly and tested with mock data.
"""

import json
import pytest

from ai_suggester.quarantine_analyzer import _build_prompt, _parse_response


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

PATTERNS_FULL = {
    "total_quarantined": 200,
    "violation_counts": [
        {
            "rule_name": "quantity_min_value",
            "message": "quantity must be >= 1",
            "function": "min_value",
            "count": 120,
            "pct_of_quarantine": 60.0,
            "sample_offending_values": {
                "quantity": ["-1", "0", "-5"],
                "price": ["9.99", "14.99"],
            },
        },
        {
            "rule_name": "event_id_not_null",
            "message": "event_id must not be null",
            "function": "is_not_null",
            "count": 80,
            "pct_of_quarantine": 40.0,
            "sample_offending_values": {
                "quantity": ["2", "3"],
                "price": ["5.00"],
            },
        },
    ],
}

PATTERNS_EMPTY = {"total_quarantined": 0, "violation_counts": []}


VALID_LLM_RESPONSE = json.dumps([
    {
        "name": "quantity_not_zero",
        "column": "quantity",
        "function": "min_value",
        "constraint": {"min": 1},
        "criticality": "error",
        "message": "quantity must be positive",
        "reasoning": "Negative quantities indicate data entry errors.",
    }
])

RESPONSE_WITH_MARKDOWN = f"```json\n{VALID_LLM_RESPONSE}\n```"

RESPONSE_BAD_JSON = "Here are your rules: sure thing!"

RESPONSE_MISSING_KEYS = json.dumps([
    {
        "name": "incomplete_rule",
        "column": "price",
        # missing: function, constraint, criticality, message, reasoning
    }
])

RESPONSE_BAD_CRITICALITY = json.dumps([
    {
        "name": "some_rule",
        "column": "store_id",
        "function": "is_not_null",
        "constraint": {},
        "criticality": "critical",    # invalid — should be defaulted to "warning"
        "message": "store_id required",
        "reasoning": "All records need a store.",
    }
])


# ---------------------------------------------------------------------------
# _build_prompt tests
# ---------------------------------------------------------------------------

def test_build_prompt_contains_rule_name():
    prompt = _build_prompt(PATTERNS_FULL, "Retail POS sales")
    assert "quantity_min_value" in prompt


def test_build_prompt_contains_pct_of_quarantine():
    prompt = _build_prompt(PATTERNS_FULL, "Retail POS sales")
    assert "60.0%" in prompt


def test_build_prompt_contains_sample_values():
    prompt = _build_prompt(PATTERNS_FULL, "Retail POS sales")
    assert "-1" in prompt or "0" in prompt


def test_build_prompt_contains_context():
    prompt = _build_prompt(PATTERNS_FULL, "Retail POS sales")
    assert "Retail POS sales" in prompt


def test_build_prompt_contains_total_count():
    prompt = _build_prompt(PATTERNS_FULL, "Retail POS sales")
    assert "200" in prompt


def test_build_prompt_empty_patterns():
    prompt = _build_prompt(PATTERNS_EMPTY, "Some dataset")
    assert "0" in prompt
    assert "Some dataset" in prompt


# ---------------------------------------------------------------------------
# _parse_response tests
# ---------------------------------------------------------------------------

def test_parse_response_valid_json():
    rules = _parse_response(VALID_LLM_RESPONSE, "sales_batch_rules", "openrouter")
    assert len(rules) == 1
    assert rules[0]["name"] == "quantity_not_zero"


def test_parse_response_sets_source_quarantine_analysis():
    rules = _parse_response(VALID_LLM_RESPONSE, "sales_batch_rules", "openrouter")
    assert rules[0]["source"] == "quarantine_analysis"


def test_parse_response_sets_llm_backend():
    rules = _parse_response(VALID_LLM_RESPONSE, "sales_batch_rules", "claude")
    assert rules[0]["llm_backend"] == "claude"


def test_parse_response_sets_confidence_none():
    rules = _parse_response(VALID_LLM_RESPONSE, "sales_batch_rules", "openrouter")
    assert rules[0]["confidence"] is None


def test_parse_response_strips_markdown_fences():
    rules = _parse_response(RESPONSE_WITH_MARKDOWN, "sales_batch_rules", "openrouter")
    assert len(rules) == 1
    assert rules[0]["name"] == "quantity_not_zero"


def test_parse_response_bad_json_returns_empty():
    rules = _parse_response(RESPONSE_BAD_JSON, "sales_batch_rules", "openrouter")
    assert rules == []


def test_parse_response_skips_rules_missing_required_keys():
    rules = _parse_response(RESPONSE_MISSING_KEYS, "sales_batch_rules", "openrouter")
    assert rules == []


def test_parse_response_defaults_bad_criticality_to_warning():
    rules = _parse_response(RESPONSE_BAD_CRITICALITY, "sales_batch_rules", "openrouter")
    assert len(rules) == 1
    assert rules[0]["criticality"] == "warning"
