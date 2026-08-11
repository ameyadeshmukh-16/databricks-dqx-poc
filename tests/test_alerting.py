"""
Unit tests for ai_suggester.alerting.

format_slack_message is pure Python and tested directly. send_slack_alert,
write_alert_log, and run_alert_check are tested with mocked Spark/requests,
following the same pattern as tests/test_rule_metrics.py.

Run with: pytest tests/test_alerting.py -v
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# format_slack_message — pure Python, no Spark
# ---------------------------------------------------------------------------

def test_format_slack_message_empty_spikes():
    from ai_suggester.alerting import format_slack_message
    payload = format_slack_message([])
    assert "blocks" in payload
    assert "0 rule(s)" in payload["blocks"][0]["text"]["text"]


def test_format_slack_message_includes_rule_name():
    from ai_suggester.alerting import format_slack_message
    spikes = [{
        "rule_name": "price_positive", "run_config_name": "sales_batch_rules",
        "run_id": "r1", "baseline_pct": 4.0, "last_run_pct": 22.5, "multiplier_used": 2.5,
    }]
    payload = format_slack_message(spikes)
    section_text = payload["blocks"][1]["text"]["text"]
    assert "price_positive" in section_text
    assert "22.5%" in section_text
    assert "4.0%" in section_text


def test_format_slack_message_multiple_spikes_one_per_line():
    from ai_suggester.alerting import format_slack_message
    spikes = [
        {"rule_name": "a", "run_config_name": "cfg", "run_id": "r1",
         "baseline_pct": 1.0, "last_run_pct": 10.0, "multiplier_used": 2.5},
        {"rule_name": "b", "run_config_name": "cfg", "run_id": "r2",
         "baseline_pct": 2.0, "last_run_pct": 15.0, "multiplier_used": 2.5},
    ]
    payload = format_slack_message(spikes)
    section_text = payload["blocks"][1]["text"]["text"]
    assert section_text.count("\n") == 1
    assert "a" in section_text and "b" in section_text


def test_format_slack_message_header_count():
    from ai_suggester.alerting import format_slack_message
    spikes = [{"rule_name": "x", "run_config_name": "cfg", "run_id": "r1",
               "baseline_pct": 1.0, "last_run_pct": 10.0, "multiplier_used": 2.5}]
    payload = format_slack_message(spikes)
    assert "1 rule(s)" in payload["blocks"][0]["text"]["text"]


# ---------------------------------------------------------------------------
# send_slack_alert — requests mocked
# ---------------------------------------------------------------------------

def test_send_slack_alert_success():
    from ai_suggester.alerting import send_slack_alert

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("requests.post", return_value=mock_response) as mock_post:
        success, error = send_slack_alert({"blocks": []}, "https://hooks.slack.com/x")

    assert success is True
    assert error is None
    mock_post.assert_called_once()


def test_send_slack_alert_failure_returns_error():
    import requests
    from ai_suggester.alerting import send_slack_alert

    with patch("requests.post", side_effect=requests.RequestException("timeout")):
        success, error = send_slack_alert({"blocks": []}, "https://hooks.slack.com/x")

    assert success is False
    assert error == "timeout"


# ---------------------------------------------------------------------------
# write_alert_log — Spark mocked
# ---------------------------------------------------------------------------

def _stub_pyspark(monkeypatch):
    pyspark_stub = MagicMock()
    pyspark_stub.sql.types.StructType = MagicMock(return_value=MagicMock())
    monkeypatch.setitem(sys.modules, "pyspark", pyspark_stub)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_stub.sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", pyspark_stub.sql.types)


def test_write_alert_log_empty_spikes_returns_zero():
    from ai_suggester.alerting import write_alert_log
    mock_spark = MagicMock()
    result = write_alert_log([], notified=True, error=None, spark=mock_spark)
    assert result == 0
    mock_spark.createDataFrame.assert_not_called()


def test_write_alert_log_writes_one_row_per_spike(monkeypatch):
    _stub_pyspark(monkeypatch)
    from ai_suggester.alerting import write_alert_log

    spikes = [
        {"rule_name": "a", "run_config_name": "cfg", "run_id": "r1",
         "baseline_pct": 1.0, "last_run_pct": 10.0, "multiplier_used": 2.5},
        {"rule_name": "b", "run_config_name": "cfg", "run_id": "r2",
         "baseline_pct": 2.0, "last_run_pct": 15.0, "multiplier_used": 2.5},
    ]

    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.write.format.return_value.mode.return_value.saveAsTable.return_value = None

    result = write_alert_log(spikes, notified=True, error=None, spark=mock_spark)

    assert result == 2
    mock_spark.createDataFrame.assert_called_once()


# ---------------------------------------------------------------------------
# run_alert_check — orchestration, Spark + requests mocked
# ---------------------------------------------------------------------------

def test_run_alert_check_no_spikes_returns_empty(monkeypatch):
    from ai_suggester import alerting

    mock_spark = MagicMock()
    with patch.object(alerting, "detect_spikes", return_value=[]) as mock_detect, \
         patch.object(alerting, "write_alert_log") as mock_write:
        result = alerting.run_alert_check(mock_spark, "sales_batch_rules", "https://hook")

    assert result == []
    mock_detect.assert_called_once()
    mock_write.assert_not_called()


def test_run_alert_check_missing_webhook_logs_without_notifying(monkeypatch):
    from ai_suggester import alerting

    spikes = [{"rule_name": "a", "run_config_name": "cfg", "run_id": "r1",
               "baseline_pct": 1.0, "last_run_pct": 10.0, "multiplier_used": 2.5}]
    mock_spark = MagicMock()

    with patch.object(alerting, "detect_spikes", return_value=spikes), \
         patch.object(alerting, "write_alert_log") as mock_write:
        result = alerting.run_alert_check(mock_spark, "sales_batch_rules", "")

    assert result == spikes
    mock_write.assert_called_once_with(spikes, notified=False, error="webhook_url not configured", spark=mock_spark)


def test_run_alert_check_sends_and_logs_success(monkeypatch):
    from ai_suggester import alerting

    spikes = [{"rule_name": "a", "run_config_name": "cfg", "run_id": "r1",
               "baseline_pct": 1.0, "last_run_pct": 10.0, "multiplier_used": 2.5}]
    mock_spark = MagicMock()

    with patch.object(alerting, "detect_spikes", return_value=spikes), \
         patch.object(alerting, "send_slack_alert", return_value=(True, None)) as mock_send, \
         patch.object(alerting, "write_alert_log") as mock_write:
        result = alerting.run_alert_check(mock_spark, "sales_batch_rules", "https://hook")

    assert result == spikes
    mock_send.assert_called_once()
    mock_write.assert_called_once_with(spikes, notified=True, error=None, spark=mock_spark)
