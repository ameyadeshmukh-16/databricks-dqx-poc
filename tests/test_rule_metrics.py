"""
Unit tests for ai_suggester.rule_metrics.

Tests the pure-Python dict-building helper (_build_metrics_from_rows) without
requiring PySpark. The Spark-dependent paths (compute_rule_metrics, write_rule_metrics)
are covered by lightweight mocks for the isEmpty / write paths.

Run with: pytest tests/test_rule_metrics.py -v
"""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(rule_name: str, function: str, violations: int):
    """Build a dict-like object mimicking a Spark Row."""
    return {"rule_name": rule_name, "function": function, "violations": violations}


_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# _build_metrics_from_rows — pure Python, no Spark
# ---------------------------------------------------------------------------

def test_build_metrics_empty_rows_returns_empty_list():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    result = _build_metrics_from_rows([], 100, "run-1", "batch", "cfg", _NOW)
    assert result == []


def test_build_metrics_returns_one_dict_per_row():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [
        _make_row("price_positive", "min_value", 5),
        _make_row("event_id_not_null", "is_not_null", 2),
    ]
    result = _build_metrics_from_rows(rows, 100, "run-1", "batch", "cfg", _NOW)
    assert len(result) == 2


def test_build_metrics_violation_counts_correct():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [_make_row("qty_check", "min_value", 7)]
    result = _build_metrics_from_rows(rows, 200, "run-2", "batch", "cfg", _NOW)
    assert result[0]["violations"] == 7
    assert result[0]["total_records"] == 200


def test_build_metrics_pct_calculation():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [_make_row("r", "f", 10)]
    result = _build_metrics_from_rows(rows, 200, "run-3", "batch", "cfg", _NOW)
    assert result[0]["violation_pct"] == pytest.approx(5.0, abs=0.01)


def test_build_metrics_zero_total_records_gives_zero_pct():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [_make_row("r", "f", 3)]
    result = _build_metrics_from_rows(rows, 0, "run-4", "batch", "cfg", _NOW)
    assert result[0]["violation_pct"] == 0.0


def test_build_metrics_run_id_and_type_propagate():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [_make_row("r", "f", 1)]
    result = _build_metrics_from_rows(rows, 50, "my-run-id", "streaming", "streaming_event_rules", _NOW)
    assert result[0]["run_id"] == "my-run-id"
    assert result[0]["run_type"] == "streaming"
    assert result[0]["run_config_name"] == "streaming_event_rules"


def test_build_metrics_recorded_at_propagated():
    from ai_suggester.rule_metrics import _build_metrics_from_rows
    rows = [_make_row("r", "f", 1)]
    result = _build_metrics_from_rows(rows, 10, "run-5", "batch", "cfg", _NOW)
    assert result[0]["recorded_at"] == _NOW


# ---------------------------------------------------------------------------
# write_rule_metrics — Spark mocked
# ---------------------------------------------------------------------------

def test_write_metrics_empty_list_returns_zero():
    from ai_suggester.rule_metrics import write_rule_metrics
    mock_spark = MagicMock()
    result = write_rule_metrics([], mock_spark)
    assert result == 0
    mock_spark.createDataFrame.assert_not_called()


def test_write_metrics_returns_row_count():
    """write_rule_metrics should return len(metrics) and call createDataFrame once."""
    from ai_suggester.rule_metrics import write_rule_metrics
    import sys

    # Stub out pyspark so the import inside write_rule_metrics doesn't fail locally
    pyspark_stub = MagicMock()
    pyspark_stub.sql.types.StructType = MagicMock(return_value=MagicMock())
    with pytest.MonkeyPatch().context() as mp:
        mp.setitem(sys.modules, "pyspark", pyspark_stub)
        mp.setitem(sys.modules, "pyspark.sql", pyspark_stub.sql)
        mp.setitem(sys.modules, "pyspark.sql.types", pyspark_stub.sql.types)

        metrics = [
            {
                "run_id": "r1", "run_type": "batch", "run_config_name": "cfg",
                "rule_name": "rule_a", "function": "is_not_null",
                "violations": 3, "total_records": 100, "violation_pct": 3.0,
                "recorded_at": datetime.now(tz=timezone.utc),
            }
        ]

        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df
        mock_df.write.format.return_value.mode.return_value.saveAsTable.return_value = None

        result = write_rule_metrics(metrics, mock_spark)

    assert result == 1
    mock_spark.createDataFrame.assert_called_once()
