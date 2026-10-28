"""Tests for the pre-flight check's pure evaluation and reporting."""

from src.preflight import (
    FAIL, INFO, OK, WARN, Check,
    compare_files, evaluate_cluster, evaluate_deployed_code, evaluate_environment, evaluate_git,
    evaluate_jobs, evaluate_secrets, evaluate_warehouse, exit_code, format_report, normalize_text,
    parse_git_status,
)


def _by_status(checks):
    return {c.name: c.status for c in checks}


# ---------------------------------------------------------------------------
# file comparison
# ---------------------------------------------------------------------------

def test_line_endings_and_trailing_whitespace_do_not_count_as_differences():
    assert normalize_text(b"a = 1\r\nb = 2\r\n\r\n") == normalize_text("a = 1\nb = 2")


def test_real_content_differences_are_found():
    assert normalize_text("a = 1") != normalize_text("a = 2")


def test_compare_lists_changed_and_missing_files():
    local = {"src/a.py": "x", "src/b.py": "y", "src/c.py": "z"}
    remote = {"src/a.py": "x", "src/b.py": "DIFFERENT", "src/c.py": None}
    assert compare_files(local, remote) == {"changed": ["src/b.py"], "missing": ["src/c.py"]}


def test_identical_code_is_ok_and_drift_fails_with_the_fix():
    ok = evaluate_deployed_code({"changed": [], "missing": []}, total=40)
    assert ok.status == OK and "40 files" in ok.detail
    bad = evaluate_deployed_code({"changed": ["src/a.py"], "missing": ["src/new.py"]}, total=40)
    assert bad.status == FAIL and "src/a.py" in bad.detail and "bundle deploy" in bad.detail


def test_long_drift_lists_are_shortened():
    bad = evaluate_deployed_code({"changed": [f"f{i}.py" for i in range(10)], "missing": []}, total=10)
    assert "(+4 more)" in bad.detail


# ---------------------------------------------------------------------------
# secrets, jobs, cluster, warehouse
# ---------------------------------------------------------------------------

def test_all_required_secrets_present():
    checks = evaluate_secrets({"azure-storage-key", "eventhub-conn-str", "openrouter-api-key", "slack-webhook-url"})
    assert all(c.status == OK for c in checks)


def test_missing_required_secret_fails_and_missing_slack_only_warns():
    checks = evaluate_secrets({"azure-storage-key", "eventhub-conn-str"})
    statuses = [c.status for c in checks]
    assert FAIL in statuses and "openrouter-api-key" in checks[0].detail
    assert evaluate_secrets({"azure-storage-key", "eventhub-conn-str", "openrouter-api-key"})[1].status == WARN


def test_jobs_found_by_display_name_suffix():
    names = ["[dev me] [RTM] Kafka Streaming Pipeline", "[dev me] [RTM] Review Rule", "[dev me] [RTM] Live Status"]
    checks = evaluate_jobs(names)
    assert checks[0].status == OK
    assert any(c.name == "Other jobs" and c.status == WARN for c in checks)


def test_missing_required_job_fails():
    assert evaluate_jobs(["[dev me] [RTM] Review Rule"])[0].status == FAIL


def test_cluster_states():
    assert evaluate_cluster("RUNNING").status == OK
    assert "started by this check" in evaluate_cluster("RUNNING", started_by_us=True).detail
    assert evaluate_cluster("PENDING").status == WARN
    stopped = evaluate_cluster("TERMINATED")
    assert stopped.status == FAIL and "--start-cluster" in stopped.detail
    assert evaluate_cluster(None).status == FAIL


def test_warehouse_should_be_stopped():
    assert evaluate_warehouse("STOPPED").status == OK
    assert evaluate_warehouse("RUNNING").status == WARN


# ---------------------------------------------------------------------------
# git and environment
# ---------------------------------------------------------------------------

def test_git_status_parsing_separates_tracked_from_untracked():
    out = " M src/a.py\n?? notes.md\nA  src/new.py\n"
    assert parse_git_status(out) == {"tracked": ["src/a.py", "src/new.py"], "untracked": ["notes.md"]}


def test_git_evaluation():
    assert evaluate_git({"tracked": [], "untracked": []}).status == OK
    assert "untracked" in evaluate_git({"tracked": [], "untracked": ["x"]}).detail
    dirty = evaluate_git({"tracked": ["src/a.py"], "untracked": []}, ahead=2)
    assert dirty.status == WARN and "uncommitted" in dirty.detail and "2 commit" in dirty.detail


def test_empty_environment_is_informational():
    check = evaluate_environment({"registry_rules": None, "stream_clean": None})
    assert check.status == INFO and "empty" in check.detail


def test_leftover_data_suggests_a_reset():
    check = evaluate_environment({"registry_rules": 12, "pending": 22, "stream_clean": 1000, "stream_quarantine": 500})
    assert check.status == INFO and "Reset Demo" in check.detail and "1,500" in check.detail


def test_seeded_but_unused_environment_needs_no_reset():
    check = evaluate_environment({"registry_rules": 12, "pending": 0, "stream_clean": 0, "stream_quarantine": 0})
    assert "Reset Demo" not in check.detail


# ---------------------------------------------------------------------------
# result handling
# ---------------------------------------------------------------------------

def test_exit_code_is_non_zero_only_for_failures():
    assert exit_code([Check("a", OK), Check("b", WARN), Check("c", INFO)]) == 0
    assert exit_code([Check("a", OK), Check("b", FAIL)]) == 1


def test_report_verdicts():
    assert "READY: all checks passed" in format_report([Check("a", OK, "fine")])
    assert "READY with 1 warning" in format_report([Check("a", WARN, "hmm")])
    report = format_report([Check("cluster", FAIL, "stopped"), Check("x", OK)])
    assert "NOT READY: 1 problem" in report and "[FAIL] cluster" in report


def test_report_uses_plain_ascii_markers_so_any_console_can_print_it():
    report = format_report([Check("a", OK), Check("b", INFO), Check("c", WARN), Check("d", FAIL)])
    report.encode("ascii")


# ---------------------------------------------------------------------------
# deep checks
# ---------------------------------------------------------------------------

from src.preflight import deep_check_code, evaluate_deep, pinned_models


def test_pinned_models_come_from_the_bundle_default():
    bundle = {"variables": {"llm_model": {"default": "a:free,b:free"}}}
    assert pinned_models(bundle) == "a:free,b:free"
    assert pinned_models({}) == "" and pinned_models({"variables": {}}) == ""


def test_the_deep_check_code_is_valid_python_and_carries_the_models():
    code = deep_check_code("poolside/x:free,google/y:free")
    compile(code, "<deep>", "exec")
    assert "poolside/x:free,google/y:free" in code and "@@RESULT@@" in code


def test_deep_results_become_checks():
    ok = evaluate_deep({"eventhub": "reachable (0.1s)", "llm": "openrouter:m answered in 3s"})
    assert [c.status for c in ok] == [OK, OK]
    bad = evaluate_deep({"eventhub_error": "timed out", "llm_error": "empty message"})
    assert [c.status for c in bad] == [FAIL, FAIL] and "timed out" in bad[0].detail
