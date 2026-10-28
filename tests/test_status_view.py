"""Tests for the status snapshot's pure formatting."""

from src.status_view import format_table


def test_empty_rows_use_the_empty_message():
    assert format_table(["a", "b"], []) == "  (nothing yet)"


def test_custom_empty_message():
    assert format_table(["a"], [], empty_msg="  (none)") == "  (none)"


def test_header_and_rows_are_rendered():
    out = format_table(["rule", "pct"], [["quantity_positive", 7.5], ["division_allowlist", 30.0]])
    lines = out.splitlines()
    assert lines[0] == "  rule | pct"
    assert lines[1] == "  quantity_positive | 7.5"
    assert lines[2] == "  division_allowlist | 30.0"


def test_none_values_render_as_text():
    assert format_table(["x"], [[None]]).splitlines()[1] == "  None"
