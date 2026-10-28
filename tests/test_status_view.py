"""Tests for the one-screen status view (pure functions only)."""

from datetime import datetime, timezone

from src.status_view import bar, build_screen, fmt_int, pct, recent_pcts, since_last, trunc, user_part

TS = datetime(2026, 10, 28, 6, 29, 21, tzinfo=timezone.utc)
APPROVED_AT = datetime(2026, 10, 28, 6, 27, 22)


def _snap(**over):
    snap = {
        "ts": TS,
        "clean": 1182,
        "quarantine": 1515,
        "batch_pcts": [55.0, 56.0, 60.0, 57.5],
        "rules": ["division_allowlist", "division_not_null", "quantity_positive",
                  "event_id_not_null", "unit_price_positive", "coupon_division_integrity",
                  "quantity_min_value"],
        "firing": {"division_allowlist": 30.0, "division_not_null": 18.3, "quantity_positive": 7.5,
                   "quantity_min_value": 3.3},
        "reviewed": [{"name": "quantity_min_value", "reviewed_by": "ameyad@bitwiseglobal.net",
                      "reviewed_at": APPROVED_AT}],
        "sugg_counts": {"pending": 10, "approved": 4, "rejected": 1},
        "pending": [{"name": "quantity_not_null", "criticality": "error", "source": "statistical"},
                    {"name": "unit_price_not_null", "criticality": "error", "source": "statistical"},
                    {"name": "store_id_valid_values", "criticality": "warning", "source": "statistical"}],
        "last_notification": {"created_at": datetime(2026, 10, 28, 6, 22), "new_count": 5},
    }
    snap.update(over)
    return snap


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def test_pct_handles_zero_total():
    assert pct(5, 0) == 0.0
    assert pct(1, 4) == 25.0


def test_bar_extremes_and_visibility():
    assert bar(0, 10) == "░" * 10
    assert bar(100, 10) == "█" * 10
    assert bar(1, 10).startswith("█")          # tiny non-zero values stay visible
    assert len(bar(37.5, 10)) == 10


def test_bar_clamps_out_of_range():
    assert bar(250, 4) == "████"
    assert bar(-5, 4) == "░░░░"


def test_recent_pcts_are_plain_numbers_oldest_to_newest():
    assert recent_pcts([55.0, 56.4, 60.0]) == "55% 56% 60%"


def test_recent_pcts_keeps_only_the_latest_values():
    assert recent_pcts([1, 2, 3, 4, 5, 6, 7, 8], n=3) == "6% 7% 8%"
    assert recent_pcts([]) == ""


def test_fmt_trunc_user():
    assert fmt_int(1234567) == "1,234,567"
    assert trunc("abcdefghij", 5) == "abcd…"
    assert trunc("abc", 5) == "abc"
    assert user_part("jane.doe@corp.com") == "jane.doe"
    assert user_part(None) == "unknown"


# ---------------------------------------------------------------------------
# since_last
# ---------------------------------------------------------------------------

def test_since_last_without_previous_is_empty():
    assert since_last(None, _snap()) == []


def test_since_last_reports_events_rule_and_suggestions():
    prev = _snap(clean=1000, quarantine=1457, rules=_snap()["rules"][:-1],
                 sugg_counts={"pending": 7})
    parts = since_last(prev, _snap())
    assert "+240 events" in parts
    assert any(p.startswith("rule applied:") and "quantity_min_value" in p for p in parts)
    assert "+3 suggestions" in parts


def test_since_last_caps_how_many_new_rules_are_named():
    prev = _snap(rules=[])
    (rule_part,) = [p for p in since_last(prev, _snap(clean=0, quarantine=0)) if p.startswith("rule applied")]
    assert "more" in rule_part


def test_since_last_no_change():
    assert since_last(_snap(), _snap()) == []


# ---------------------------------------------------------------------------
# build_screen
# ---------------------------------------------------------------------------

def test_full_screen_tells_the_story():
    text = build_screen(_snap())
    assert "LIVE DATA QUALITY LOOP" in text and "06:29:21 UTC" in text
    assert "EVENTS IN" in text and "2,697 total" in text
    assert "quarantined per batch: 55% 56% 60% 58%" in text      # real numbers, not glyphs
    assert "RULES ENFORCED (7 active)" in text
    assert "ADDED BY HUMAN REVIEW" in text
    assert "approved 06:27 by ameyad" in text           # reviewer shown without the email domain
    assert "bitwiseglobal" not in text
    assert "catching 3.3%" in text
    assert "AI SUGGESTIONS" in text and "10 waiting" in text
    assert "YOUR TURN" in text and "Review Rule" in text


def test_every_line_fits_without_wrapping():
    long_names = _snap(
        pending=[{"name": "a_very_long_suggestion_name_that_keeps_going_on_and_on",
                  "criticality": "warning", "source": "quarantine_analysis"}] * 3,
        reviewed=[{"name": "another_extremely_long_rule_name_for_the_review_section",
                   "reviewed_by": "a.very.long.reviewer.name@example.com", "reviewed_at": APPROVED_AT}],
        rules=_snap()["rules"] + ["another_extremely_long_rule_name_for_the_review_section"],
        firing={"another_extremely_long_rule_name_for_the_review_section": 2.0, "division_allowlist": 30.0},
    )
    for screen in (build_screen(_snap()), build_screen(long_names, prev=_snap())):
        for line in screen.splitlines():
            assert len(line) <= 66, f"too wide ({len(line)}): {line!r}"


def test_top_rules_are_limited_and_the_rest_summarised():
    names = [f"rule_{i}" for i in range(9)]
    snap = _snap(rules=names, reviewed=[], firing={n: 10.0 - i for i, n in enumerate(names)})
    text = build_screen(snap)
    assert "rule_4" in text and "rule_5" not in text
    assert "+4 more active rules, quiet" in text


def test_quiet_rules_line_is_singular_for_one():
    names = [f"rule_{i}" for i in range(6)]
    snap = _snap(rules=names, reviewed=[], firing={n: 10.0 - i for i, n in enumerate(names[:5])})
    assert "+1 more active rule, quiet" in build_screen(snap)


def test_pending_list_is_truncated_with_more_line():
    snap = _snap(sugg_counts={"pending": 8, "approved": 0, "rejected": 0})
    assert "+5 more" in build_screen(snap)


def test_empty_snapshot_does_not_break():
    empty = _snap(clean=0, quarantine=0, batch_pcts=[], rules=[], firing={}, reviewed=[],
                  sugg_counts={}, pending=[], last_notification=None)
    text = build_screen(empty)
    assert "waiting for the first events" in text
    assert "no rule has caught an event yet" in text
    assert "0 waiting" in text


def test_delta_line_only_appears_with_a_previous_snapshot():
    assert "since last snapshot" not in build_screen(_snap())
    assert "▲ since last snapshot: no change" in build_screen(_snap(), prev=_snap())
    prev = _snap(clean=1000, quarantine=1457)
    assert "+240 events" in build_screen(_snap(), prev=prev)
