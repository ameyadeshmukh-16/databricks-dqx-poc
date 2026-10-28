"""Tests for the reset job's safety rails and its report."""

import pytest

from src.reset_plan import (
    ResetRefused, active_demo_runs, assert_dev_catalog, assert_nothing_running, build_plan,
    describe_plan, is_confirmed,
)


# ---------------------------------------------------------------------------
# confirmation word
# ---------------------------------------------------------------------------

def test_only_the_exact_word_confirms():
    assert is_confirmed("RESET")


@pytest.mark.parametrize("value", ["", None, "reset", "Reset", " RESET", "RESET ", "RESET!", "yes", "true", "RESETRESET"])
def test_anything_else_is_a_dry_run(value):
    assert not is_confirmed(value)
    assert build_plan("c.s", {}, [], value)["mode"] == "dry-run"


def test_confirmed_plan_is_marked_execute():
    assert build_plan("c.s", {}, [], "RESET")["mode"] == "execute"


# ---------------------------------------------------------------------------
# catalog rail
# ---------------------------------------------------------------------------

def test_a_dev_catalog_is_allowed():
    assert_dev_catalog("dqx_poc_dev")


@pytest.mark.parametrize("catalog", ["dqx_poc", "prod", "dqx_poc_prod", "dev_dqx_poc", "main", "dqx_poc_dev_backup"])
def test_other_catalogs_are_refused(catalog):
    with pytest.raises(ResetRefused, match="_dev"):
        assert_dev_catalog(catalog)


# ---------------------------------------------------------------------------
# active runs rail
# ---------------------------------------------------------------------------

def test_the_reset_job_does_not_block_itself():
    assert active_demo_runs(["[dev me] [RTM] Reset Demo"]) == []
    assert_nothing_running(["[dev me] [RTM] Reset Demo"])


def test_other_demo_runs_block_the_reset():
    runs = ["[dev me] [RTM] Reset Demo", "[dev me] [RTM] Kafka Streaming Pipeline", "[dev me] [RTM] Review Rule"]
    assert active_demo_runs(runs) == ["[dev me] [RTM] Kafka Streaming Pipeline", "[dev me] [RTM] Review Rule"]
    with pytest.raises(ResetRefused, match="Kafka Streaming Pipeline"):
        assert_nothing_running(runs)


def test_unrelated_runs_in_the_workspace_are_ignored():
    assert active_demo_runs(["nightly-etl", "[dev me] some other project"]) == []


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

def test_dry_run_report_lists_what_would_go_and_says_nothing_changed():
    plan = build_plan("dqx_poc_dev.retail_dqx_poc", {"stream_events_clean": 1500, "dq_rules_registry": None},
                      ["abfss://c@a.dfs.core.windows.net/checkpoints/kafka_streaming_dqx"], "")
    text = describe_plan(plan)
    assert "DRY RUN" in text and "nothing has been changed" in text and "confirm = RESET" in text
    assert "stream_events_clean  (1,500 rows)" in text and "dq_rules_registry" in text
    assert "kafka_streaming_dqx" in text


def test_confirmed_report_says_it_is_removing():
    text = describe_plan(build_plan("c.s", {"t": 1}, [], "RESET"))
    assert "RESET CONFIRMED" in text and "DRY RUN" not in text


def test_report_for_an_already_empty_environment():
    text = describe_plan(build_plan("c.s", {}, [], ""))
    assert "not present (nothing to drop)" in text and "nothing to remove" in text
