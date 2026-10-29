import pytest

from src.delta_retry import is_write_conflict, retry_on_conflict


class _Conflict(Exception):
    pass


def _conflict():
    return _Conflict("[DELTA_METADATA_CHANGED] MetadataChangedException: The metadata of the Delta table has been changed")


def test_conflict_detection():
    assert is_write_conflict(_conflict())
    assert is_write_conflict(Exception("io.delta.exceptions.ConcurrentAppendException: files were added"))
    assert not is_write_conflict(Exception("[SCHEMA_NOT_FOUND] the schema cannot be found"))


def test_success_runs_once():
    calls = []
    assert retry_on_conflict(lambda: calls.append(1) or "ok", sleep=lambda s: None) == "ok"
    assert len(calls) == 1


def test_conflict_is_retried_then_succeeds():
    state = {"n": 0}
    waits, seen = [], []

    def write():
        state["n"] += 1
        if state["n"] < 3:
            raise _conflict()
        return "done"

    result = retry_on_conflict(write, delay_seconds=2, sleep=waits.append, on_retry=lambda a, e: seen.append(a))
    assert result == "done"
    assert state["n"] == 3
    assert waits == [2, 4]            # growing delay
    assert seen == [1, 2]


def test_gives_up_after_the_attempt_limit():
    state = {"n": 0}

    def write():
        state["n"] += 1
        raise _conflict()

    with pytest.raises(_Conflict):
        retry_on_conflict(write, attempts=3, sleep=lambda s: None)
    assert state["n"] == 3


def test_other_errors_are_not_retried():
    state = {"n": 0}

    def write():
        state["n"] += 1
        raise ValueError("bad data")

    with pytest.raises(ValueError):
        retry_on_conflict(write, sleep=lambda s: None)
    assert state["n"] == 1
