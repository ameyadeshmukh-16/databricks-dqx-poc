"""Tests for the warm review session, using a fake Databricks SDK client."""

import json
from types import SimpleNamespace

import pytest

from live_board.cluster_session import MARKER, ReviewSession, ReviewSessionError, build_command, parse_result


class FakeCommands:
    def __init__(self, outputs=None, fail_times=0):
        self.created, self.destroyed, self.commands = [], [], []
        self._outputs = list(outputs or [])
        self._fail = fail_times

    def create_and_wait(self, cluster_id, language):
        self.created.append(cluster_id)
        return SimpleNamespace(id=f"ctx{len(self.created)}")

    def execute_and_wait(self, cluster_id, context_id, language, command):
        self.commands.append((context_id, command))
        if self._fail > 0 and MARKER in command:
            self._fail -= 1
            raise RuntimeError("context lost")
        data = self._outputs.pop(0) if self._outputs and MARKER in command else ""
        return SimpleNamespace(results=SimpleNamespace(result_type="text", data=data, cause=None))

    def destroy(self, cluster_id, context_id):
        self.destroyed.append(context_id)


def _client(commands):
    return SimpleNamespace(
        command_execution=commands,
        current_user=SimpleNamespace(me=lambda: SimpleNamespace(user_name="me@corp.com")),
    )


def _out(**result):
    return "noise\n" + MARKER + json.dumps({"ok": True, "message": "done", **result}) + "\n"


# ---------------------------------------------------------------------------
# command / parsing
# ---------------------------------------------------------------------------

def test_command_runs_the_shared_review_function_with_quoted_arguments():
    cmd = build_command("approve", "it's a rule", "streaming_event_rules")
    assert "from src.review_service import run_review" in cmd
    assert "'approve'" in cmd and "streaming_event_rules" in cmd
    compile(cmd, "<cmd>", "exec")                                   # valid Python even with a quote in the name


def test_unknown_action_is_rejected_before_anything_runs():
    with pytest.raises(ValueError):
        build_command("delete", "x", "g")


def test_parse_takes_the_last_marked_line_and_ignores_noise():
    out = f"log line\n{MARKER}{json.dumps({'ok': False})}\nmore log\n{MARKER}{json.dumps({'ok': True})}\n"
    assert parse_result(out) == {"ok": True}


def test_parse_without_a_result_is_an_error():
    with pytest.raises(ReviewSessionError):
        parse_result("just logs")


# ---------------------------------------------------------------------------
# session behaviour
# ---------------------------------------------------------------------------

def test_first_call_creates_one_context_and_puts_the_bundle_on_the_path():
    cmds = FakeCommands([_out()])
    session = ReviewSession(client=_client(cmds), cluster_id="c1")
    assert session.review("list")["ok"] is True
    assert cmds.created == ["c1"]
    init = cmds.commands[0][1]
    assert "/Workspace/Users/me@corp.com/.bundle/databricks-dqx-poc/dev/files" in init


def test_the_context_is_reused_across_calls():
    cmds = FakeCommands([_out(), _out()])
    session = ReviewSession(client=_client(cmds))
    session.review("list")
    session.review("approve", "a")
    assert len(cmds.created) == 1


def test_a_lost_context_is_recreated_once_and_the_call_succeeds():
    cmds = FakeCommands([_out()], fail_times=1)
    session = ReviewSession(client=_client(cmds))
    assert session.review("list")["ok"] is True
    assert len(cmds.created) == 2 and cmds.destroyed == ["ctx1"]


def test_two_failures_raise_a_clear_error():
    cmds = FakeCommands([], fail_times=5)
    session = ReviewSession(client=_client(cmds))
    with pytest.raises(ReviewSessionError, match="context lost"):
        session.review("list")


def test_an_error_result_from_the_cluster_is_reported():
    class ErrCommands(FakeCommands):
        def execute_and_wait(self, cluster_id, context_id, language, command):
            return SimpleNamespace(results=SimpleNamespace(result_type="error", data=None, cause="Traceback\nValueError: boom"))
    session = ReviewSession(client=_client(ErrCommands()))
    with pytest.raises(ReviewSessionError, match="boom"):
        session.review("list")


def test_close_destroys_the_context_and_a_later_call_makes_a_new_one():
    cmds = FakeCommands([_out(), _out()])
    session = ReviewSession(client=_client(cmds))
    session.review("list")
    session.close()
    assert cmds.destroyed == ["ctx1"]
    session.review("list")
    assert len(cmds.created) == 2


def test_close_without_a_session_is_harmless():
    ReviewSession(client=_client(FakeCommands())).close()
