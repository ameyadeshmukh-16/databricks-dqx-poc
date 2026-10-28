"""Tests for the live board's pure helpers (no Spark, no network, no Streamlit)."""

from datetime import datetime

import pytest

from live_board.data import (
    banner_for,
    batch_chart_rows,
    http_path,
    pause_status,
    reviewed_cards,
    rules_chart_rows,
    should_refresh_slow,
)

T = datetime(2026, 10, 28, 6, 27, 22)


def _snap(**over):
    snap = {
        "rules": ["division_allowlist", "quantity_positive", "quantity_min_value", "quiet_rule"],
        "firing": {"division_allowlist": 30.04, "quantity_positive": 7.5, "quantity_min_value": 3.3,
                   "retired_rule": 9.0},
        "reviewed": [{"name": "quantity_min_value", "reviewed_by": "ameyad@bitwiseglobal.net", "reviewed_at": T},
                     {"name": "not_active_any_more", "reviewed_by": "x@y.z", "reviewed_at": T}],
        "batch_history": [{"batch_id": 1, "quarantine_pct": 55.55, "total": 120, "processed_at": T},
                          {"batch_id": 2, "quarantine_pct": 60.0, "total": 120, "processed_at": T}],
    }
    snap.update(over)
    return snap


# ---------------------------------------------------------------------------
# chart rows
# ---------------------------------------------------------------------------

def test_batch_rows_are_rounded_and_ordered():
    assert batch_chart_rows(_snap()) == [{"batch": 1, "quarantine_pct": 55.5},
                                         {"batch": 2, "quarantine_pct": 60.0}]


def test_rules_rows_label_human_added_and_skip_inactive_rules():
    rows = rules_chart_rows(_snap())
    by_rule = {r["rule"]: r for r in rows}
    assert by_rule["quantity_min_value"]["added_by"] == "human review"
    assert by_rule["division_allowlist"]["added_by"] == "seeded"
    assert "retired_rule" not in by_rule                      # fired once but is no longer active
    assert [r["rule"] for r in rows][0] == "division_allowlist"   # sorted by share, highest first


def test_human_added_rule_that_has_not_fired_still_gets_a_zero_bar():
    snap = _snap(firing={"division_allowlist": 30.0})
    zero = [r for r in rules_chart_rows(snap) if r["rule"] == "quantity_min_value"]
    assert zero == [{"rule": "quantity_min_value", "caught_pct": 0.0, "added_by": "human review"}]


def test_rules_rows_respect_top_limit():
    names = [f"r{i}" for i in range(12)]
    snap = _snap(rules=names, reviewed=[], firing={n: float(i) for i, n in enumerate(names)})
    assert len(rules_chart_rows(snap, top=5)) == 5


# ---------------------------------------------------------------------------
# reviewed cards and banner
# ---------------------------------------------------------------------------

def test_reviewed_cards_only_include_live_rules_and_hide_the_email_domain():
    cards = reviewed_cards(_snap())
    assert [c["name"] for c in cards] == ["quantity_min_value"]
    assert cards[0]["by"] == "ameyad" and cards[0]["approved_at"] == "06:27"
    assert cards[0]["caught_pct"] == 3.3


def test_banner_names_newly_applied_rules():
    prev, cur = _snap(rules=["division_allowlist"]), _snap()
    text = banner_for(prev, cur)
    assert text and "quantity_min_value" in text


def test_no_banner_without_change_or_previous():
    assert banner_for(None, _snap()) is None
    assert banner_for(_snap(), _snap()) is None


# ---------------------------------------------------------------------------
# connection and cost guards
# ---------------------------------------------------------------------------

def test_cluster_http_path_uses_workspace_id_from_host():
    path = http_path("cluster", "adb-7405615968854762.2.azuredatabricks.net")
    assert path.startswith("sql/protocolv1/o/7405615968854762/")


def test_warehouse_http_path():
    assert http_path("warehouse", "anything").startswith("/sql/1.0/warehouses/")


def test_cluster_path_needs_a_recognisable_host():
    with pytest.raises(ValueError):
        http_path("cluster", "example.com")


def test_auto_pause_counts_down_then_triggers():
    assert pause_status(0, 60, 20) == (False, 1140)
    assert pause_status(0, 1200, 20) == (True, 0)
    assert pause_status(0, 5000, 20) == (True, 0)


def test_slow_data_refreshes_on_ttl():
    assert should_refresh_slow(None, 100)
    assert not should_refresh_slow(90, 100, ttl=30)
    assert should_refresh_slow(60, 100, ttl=30)
