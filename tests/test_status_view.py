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


# ---------------------------------------------------------------------------
# collect_* with a fake runner (no Spark, no network)
# ---------------------------------------------------------------------------

from decimal import Decimal

from src.status_view import collect_fast, collect_slow, collect_snapshot


class FakeRunner:
    """Answers queries by matching a keyword in the SQL; table existence is configurable."""

    def __init__(self, tables, answers):
        self.tables = set(tables)
        self.answers = answers          # {keyword: rows}
        self.calls = []

    def table_exists(self, name):
        return any(name.endswith(t) for t in self.tables)

    def query(self, sql):
        self.calls.append(sql)
        for key, rows in self.answers.items():
            if key in sql:
                return rows
        raise AssertionError(f"unexpected query: {sql[:80]}")


ALL_TABLES = ["stream_events_clean", "stream_events_quarantine", "stream_batch_metrics", "dq_rules_registry",
              "dq_rule_metrics", "dq_rules_staging", "dq_review_notifications"]


def _runner(tables=ALL_TABLES):
    t0 = datetime(2026, 10, 28, 6, 0, 0)
    return FakeRunner(tables, {
        "AS quarantine": [{"clean": 100, "quarantine": 150}],           # both counts in one query
        "stream_batch_metrics": [
            {"batch_id": 3, "quarantine_count": 6, "total_count": 10, "processed_at": t0},
            {"batch_id": 2, "quarantine_count": 5, "total_count": 10, "processed_at": t0},
            {"batch_id": 1, "quarantine_count": 4, "total_count": 10, "processed_at": t0},
        ],
        "dq_rules_registry": [{"name": "a"}, {"name": "b"}],
        "last_runs": [{"rule_name": "a", "pct": Decimal("12.5")}, {"rule_name": "b", "pct": None}],
        "status = 'approved'": [{"name": "b", "reviewed_by": "x@y.z", "reviewed_at": t0}],
        "GROUP BY status": [{"status": "pending", "n": 3}, {"status": "approved", "n": Decimal("1")}],
        "status = 'pending'": [{"name": "p", "criticality": "error", "source": "llm"}],
        "dq_review_notifications": [{"created_at": t0, "new_count": 5}],
    })


def test_collect_fast_counts_and_history_oldest_first():
    fast = collect_fast(_runner(), last_batches=2)
    assert fast["clean"] == 100 and fast["quarantine"] == 150
    assert [b["batch_id"] for b in fast["batch_history"]] == [1, 2, 3]       # oldest -> newest
    assert fast["batch_history"][-1]["quarantine_pct"] == 60.0
    assert fast["batch_pcts"] == [50.0, 60.0]                                  # last 2 only


def test_collect_slow_shapes_rows_and_converts_numbers():
    slow = collect_slow(_runner())
    assert slow["rules"] == ["a", "b"]
    assert slow["firing"] == {"a": 12.5}                                       # Decimal -> float, None dropped
    assert isinstance(slow["firing"]["a"], float)
    assert slow["reviewed"][0]["name"] == "b"
    assert slow["sugg_counts"] == {"pending": 3, "approved": 1}
    assert slow["pending"][0]["source"] == "llm"
    assert slow["last_notification"]["new_count"] == 5


def test_missing_tables_give_empty_sections_not_errors():
    snap = collect_snapshot(FakeRunner([], {}))
    assert snap["clean"] == 0 and snap["quarantine"] == 0
    assert snap["batch_history"] == [] and snap["rules"] == [] and snap["firing"] == {}
    assert snap["sugg_counts"] == {} and snap["last_notification"] is None
    assert "waiting for the first events" in build_screen(snap)


def test_counts_use_one_query_when_both_tables_exist():
    runner = _runner()
    collect_fast(runner)
    assert sum("COUNT(*)" in c for c in runner.calls) == 1


def test_counts_fall_back_to_single_table_queries():
    runner = FakeRunner(["stream_events_clean"], {"COUNT(*) AS n": [{"n": 42}]})
    fast = collect_fast(runner)
    assert fast["clean"] == 42 and fast["quarantine"] == 0


def test_fast_collection_does_not_touch_the_slow_tables():
    runner = _runner()
    collect_fast(runner)
    assert not any("dq_rules" in c or "dq_rule_metrics" in c for c in runner.calls)


def test_full_snapshot_renders_a_screen():
    text = build_screen(collect_snapshot(_runner()))
    assert "250 total" in text and "ADDED BY HUMAN REVIEW" in text


# ---------------------------------------------------------------------------
# detail=True: latest quarantined events and the decision log
# ---------------------------------------------------------------------------

def _detail_runner(tables=ALL_TABLES):
    t0 = datetime(2026, 10, 28, 6, 0, 0)
    runner = _runner(tables)
    runner.answers = {
        "transform(_errors": [{"event_id": "E1", "store_id": "S1", "division": "011", "quantity": -1,
                               "unit_price": 1.5, "ingested_at": t0, "rules": "quantity_positive",
                               "reason": "Value '-1' is less than limit: 1"}],
        "s.status <> 'pending'": [{"name": "a", "status": "approved", "criticality": "error", "source": "llm",
                                   "llm_backend": "openrouter:m", "reviewed_by": "x@y.z", "reviewed_at": t0,
                                   "first_caught_at": None}],
        "status <> 'pending'": [{"name": "b", "status": "rejected", "criticality": "error", "source": "llm",
                                 "llm_backend": None, "reviewed_by": "x@y.z", "reviewed_at": t0,
                                 "first_caught_at": None}],
        **runner.answers,
    }
    return runner


def test_detail_is_off_by_default_and_costs_no_extra_queries():
    runner = _detail_runner()
    slow = collect_slow(runner)
    assert slow["recent_quarantine"] == [] and slow["decisions"] == []
    assert not any("transform(_errors" in c or "<> 'pending'" in c for c in runner.calls)


def test_detail_reads_the_latest_events_and_decisions():
    slow = collect_slow(_detail_runner(), detail=True)
    assert slow["recent_quarantine"][0]["rules"] == "quantity_positive"
    assert slow["decisions"][0]["name"] == "a"


def test_decisions_still_work_when_the_metrics_table_does_not_exist_yet():
    tables = [t for t in ALL_TABLES if t != "dq_rule_metrics"]
    slow = collect_slow(_detail_runner(tables), detail=True)
    assert slow["decisions"][0]["name"] == "b"            # the query without the first-catch lookup


def test_detail_with_missing_tables_is_empty_not_an_error():
    slow = collect_slow(FakeRunner([], {}), detail=True)
    assert slow["recent_quarantine"] == [] and slow["decisions"] == []
