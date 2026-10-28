"""Tests for the review decision logic (pure: all side effects are injected fakes)."""

from types import SimpleNamespace

from src.review_service import decide


def _row(rule_id, name, function="is_not_null", column="quantity", constraint="{}", criticality="error"):
    return {"rule_id": rule_id, "name": name, "function": function, "column_name": column,
            "constraint": constraint, "criticality": criticality, "message": "m", "source": "llm",
            "confidence": None, "reasoning": "r"}


class Recorder:
    def __init__(self, validate_errors=None):
        self.promoted, self.rejected = [], []
        self._errors = validate_errors or []

    def promote(self, rule_id, who):
        self.promoted.append((rule_id, who))

    def reject(self, rule_id, who):
        self.rejected.append((rule_id, who))

    def validate(self, checks):
        return SimpleNamespace(has_errors=bool(self._errors), errors=self._errors)


def _run(action, ref, pending, rec=None, columns=("event_id", "quantity"), live=frozenset()):
    rec = rec or Recorder()
    out = decide(action, ref, pending, list(columns) if columns is not None else None, set(live),
                 reviewer="ameyad@x.com", promote=rec.promote, reject=rec.reject, validate=rec.validate)
    return out, rec


def test_list_changes_nothing():
    out, rec = _run("list", "", [_row("1", "a")])
    assert out["ok"] and not rec.promoted and not rec.rejected


def test_approve_promotes_with_the_reviewer():
    out, rec = _run("approve", "a", [_row("1", "a")])
    assert out["ok"] and rec.promoted == [("1", "ameyad@x.com")]
    assert "Approved 'a'" in out["message"]


def test_reject_rejects_and_never_promotes():
    out, rec = _run("reject", "a", [_row("1", "a")])
    assert out["ok"] and rec.rejected == [("1", "ameyad@x.com")] and not rec.promoted


def test_unknown_rule_is_refused_with_the_pending_names():
    out, rec = _run("approve", "zzz", [_row("1", "a"), _row("2", "b")])
    assert not out["ok"] and "Pending: a, b" in out["message"] and not rec.promoted


def test_missing_column_is_refused_and_nothing_is_written():
    out, rec = _run("approve", "t", [_row("1", "t", column="transaction_id")])
    assert not out["ok"] and "does not exist" in out["message"] and not rec.promoted


def test_dqx_rejection_is_refused_with_its_reason():
    out, rec = _run("approve", "bad", [_row("1", "bad", "sql_expression", "")],
                    rec=Recorder(validate_errors=["Missing required argument 'expression'"]))
    assert not out["ok"] and "Missing required argument" in out["message"] and not rec.promoted


def test_duplicate_of_a_live_rule_name_is_refused():
    out, rec = _run("approve", "a", [_row("1", "a")], live={"a"})
    assert not out["ok"] and "already live" in out["message"] and not rec.promoted


def test_reject_is_allowed_even_if_the_name_is_live():
    out, rec = _run("reject", "a", [_row("1", "a")], live={"a"})
    assert out["ok"] and rec.rejected


def test_unknown_action_is_refused():
    out, _ = _run("delete", "a", [_row("1", "a")])
    assert not out["ok"] and "Unknown action" in out["message"]


def test_column_check_is_skipped_when_the_target_table_is_unknown():
    out, rec = _run("approve", "a", [_row("1", "a", column="whatever")], columns=None)
    assert out["ok"] and rec.promoted
