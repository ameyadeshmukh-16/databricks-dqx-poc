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


# ---------------------------------------------------------------------------
# latest quarantined events + decision log
# ---------------------------------------------------------------------------

import csv
import io
from datetime import timezone

from live_board.data import (
    DASH, decision_log_csv, decision_rows, quarantine_table_rows, seconds_to_first_catch,
)


def _event(**over):
    row = {"event_id": "EVT1", "store_id": "STORE02", "division": "011", "quantity": -1, "unit_price": 146.6,
           "ingested_at": datetime(2026, 10, 28, 10, 0, 31), "rules": "quantity_positive, quantity_integer",
           "reason": "Value '-1' in Column 'quantity' is less than limit: 1"}
    row.update(over)
    return row


def _decision(**over):
    row = {"name": "quantity_min_value", "status": "approved", "criticality": "error", "source": "statistical",
           "llm_backend": None, "reviewed_by": "ameyad@bitwiseglobal.net",
           "reviewed_at": datetime(2026, 10, 28, 6, 27, 22), "first_caught_at": datetime(2026, 10, 28, 6, 28, 16)}
    row.update(over)
    return row


def test_quarantine_rows_show_the_event_the_rules_and_the_reason():
    (row,) = quarantine_table_rows({"recent_quarantine": [_event()]})
    assert row["time (UTC)"] == "10:00:31" and row["event"] == "EVT1" and row["price"] == "146.60"
    assert row["caught by"] == "quantity_positive, quantity_integer" and "less than limit" in row["why"]


def test_missing_values_become_a_dash_not_none():
    (row,) = quarantine_table_rows({"recent_quarantine": [_event(division=None, quantity=None, unit_price=None,
                                                                 reason=None)]})
    assert row["division"] == DASH and row["qty"] == DASH and row["price"] == DASH and row["why"] == DASH


def test_no_events_gives_an_empty_table():
    assert quarantine_table_rows({}) == [] and quarantine_table_rows({"recent_quarantine": []}) == []


def test_time_zone_aware_timestamps_are_shown_in_utc():
    from datetime import timedelta
    tz = timezone(timedelta(hours=5, minutes=30))
    (row,) = quarantine_table_rows({"recent_quarantine": [_event(ingested_at=datetime(2026, 10, 28, 15, 30, 0, tzinfo=tz))]})
    assert row["time (UTC)"] == "10:00:00"


def test_seconds_to_first_catch():
    assert seconds_to_first_catch(_decision()) == 54
    assert seconds_to_first_catch(_decision(first_caught_at=None)) is None
    assert seconds_to_first_catch(_decision(reviewed_at=None)) is None
    assert seconds_to_first_catch(_decision(first_caught_at=datetime(2026, 10, 28, 6, 0, 0))) == 0   # never negative


def test_mixed_aware_and_naive_timestamps_do_not_crash():
    aware = datetime(2026, 10, 28, 6, 28, 16, tzinfo=timezone.utc)
    assert seconds_to_first_catch(_decision(first_caught_at=aware)) == 54


def test_decision_rows_for_the_screen():
    rows = decision_rows({"decisions": [_decision(), _decision(name="x", status="rejected", first_caught_at=None,
                                                               llm_backend="openrouter:poolside/laguna-s-2.1:free")]})
    assert rows[0]["decision"] == "Approved" and rows[0]["reviewer"] == "ameyad"
    assert rows[0]["first caught after"] == "54 s" and rows[0]["suggested by"] == "statistical analysis"
    assert rows[1]["decision"] == "Rejected" and rows[1]["first caught after"] == DASH
    assert rows[1]["suggested by"] == "openrouter:poolside/laguna-s-2.1:free"
    assert "bitwiseglobal" not in str(rows)


def test_csv_keeps_the_full_identity_and_exact_times():
    text = decision_log_csv({"decisions": [_decision()]})
    (header, row) = list(csv.reader(io.StringIO(text)))
    assert header[0] == "decided_at_utc" and header[-1] == "seconds_to_first_catch"
    assert row[0] == "2026-10-28 06:27:22" and row[6] == "ameyad@bitwiseglobal.net"
    assert row[7] == "2026-10-28 06:28:16" and row[8] == "54"


def test_csv_quotes_commas_and_quotes_in_values():
    text = decision_log_csv({"decisions": [_decision(name='odd, "name"', llm_backend="a,b")]})
    row = list(csv.reader(io.StringIO(text)))[1]
    assert row[2] == 'odd, "name"' and row[5] == "a,b"


def test_csv_with_no_decisions_is_just_the_header():
    lines = decision_log_csv({"decisions": []}).strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("decided_at_utc")


def test_csv_leaves_unknown_values_empty():
    row = list(csv.reader(io.StringIO(decision_log_csv({"decisions": [_decision(first_caught_at=None, llm_backend=None)]}))))[1]
    assert row[5] == "" and row[7] == "" and row[8] == ""
