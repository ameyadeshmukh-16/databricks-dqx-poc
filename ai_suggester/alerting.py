"""
Alerting — detects rules whose violation_pct has spiked above their own
historical baseline and notifies a Slack channel via Incoming Webhook.

The feedback loop:
  dq_rule_metrics  →  detect_spikes()
                   →  format_slack_message()  →  send_slack_alert()
                   →  dq_alert_log             (audit trail + dedupe)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from config.constants import (
    ALERT_LOOKBACK_RUNS,
    ALERT_MIN_ABSOLUTE_PCT,
    ALERT_MIN_BASELINE_RUNS,
    ALERT_SPIKE_MULTIPLIER,
    TABLE_ALERT_LOG,
    TABLE_RULE_METRICS,
)
from src.logger import get_logger

logger = get_logger(__name__)

_ALERT_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_ALERT_LOG} (
    alert_id         STRING,
    rule_name        STRING,
    run_config_name  STRING,
    run_id           STRING,
    baseline_pct     DOUBLE,
    last_run_pct     DOUBLE,
    multiplier_used  DOUBLE,
    triggered_at     TIMESTAMP,
    notified         BOOLEAN,
    notify_error     STRING
) USING DELTA
"""


def ensure_alert_log_table(spark: "SparkSession") -> None:
    spark.sql(_ALERT_LOG_DDL)
    logger.debug("Alert log table ready: %s", TABLE_ALERT_LOG)


def detect_spikes(
    spark: "SparkSession",
    run_config_name: str,
    lookback_runs: int = ALERT_LOOKBACK_RUNS,
) -> list[dict]:
    """
    Compare each rule's most recent violation_pct against the average of its
    prior runs (baseline). Returns rules whose last run exceeds
    max(baseline * ALERT_SPIKE_MULTIPLIER, ALERT_MIN_ABSOLUTE_PCT), excluding
    runs already logged in dq_alert_log.

    Returns a list of dicts: rule_name, run_config_name, run_id,
    baseline_pct, last_run_pct, multiplier_used.
    """
    ensure_alert_log_table(spark)

    rows = spark.sql(f"""
        WITH ranked AS (
            SELECT
                rule_name,
                run_config_name,
                run_id,
                violation_pct,
                recorded_at,
                ROW_NUMBER() OVER (
                    PARTITION BY rule_name, run_config_name
                    ORDER BY recorded_at DESC
                ) AS rn
            FROM {TABLE_RULE_METRICS}
            WHERE run_config_name = '{run_config_name}'
        ),
        last_run AS (
            SELECT rule_name, run_config_name, run_id, violation_pct AS last_run_pct
            FROM ranked
            WHERE rn = 1
        ),
        baseline AS (
            SELECT
                rule_name,
                run_config_name,
                COUNT(*)          AS baseline_run_count,
                AVG(violation_pct) AS baseline_pct
            FROM ranked
            WHERE rn BETWEEN 2 AND {lookback_runs + 1}
            GROUP BY rule_name, run_config_name
        )
        SELECT
            l.rule_name,
            l.run_config_name,
            l.run_id,
            b.baseline_pct,
            l.last_run_pct
        FROM last_run l
        JOIN baseline b
          ON l.rule_name = b.rule_name AND l.run_config_name = b.run_config_name
        LEFT JOIN {TABLE_ALERT_LOG} a
          ON l.rule_name = a.rule_name AND l.run_id = a.run_id
        WHERE b.baseline_run_count >= {ALERT_MIN_BASELINE_RUNS}
          AND l.last_run_pct > GREATEST(b.baseline_pct * {ALERT_SPIKE_MULTIPLIER}, {ALERT_MIN_ABSOLUTE_PCT})
          AND a.alert_id IS NULL
    """).collect()

    spikes = [
        {
            "rule_name":       row["rule_name"],
            "run_config_name": row["run_config_name"],
            "run_id":          row["run_id"],
            "baseline_pct":    round(row["baseline_pct"], 2),
            "last_run_pct":    round(row["last_run_pct"], 2),
            "multiplier_used": ALERT_SPIKE_MULTIPLIER,
        }
        for row in rows
    ]

    logger.info(
        "Detected %d spike(s) for run_config_name=%s", len(spikes), run_config_name,
    )
    return spikes


def format_slack_message(spikes: list[dict]) -> dict:
    """
    Build a Slack Block Kit payload summarising the given spikes.

    Pure Python — no Spark dependency, safe to unit test directly.
    """
    header = f"🚨 Data Quality Alert — {len(spikes)} rule(s) spiked"

    lines = []
    for s in spikes:
        lines.append(
            f"*{s['rule_name']}* ({s['run_config_name']}): "
            f"{s['last_run_pct']}% this run vs. {s['baseline_pct']}% baseline "
            f"(> {s['multiplier_used']}x)"
        )

    return {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": header}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(lines)}},
        ],
    }


def send_slack_alert(payload: dict, webhook_url: str) -> tuple[bool, str | None]:
    """
    POST payload to a Slack Incoming Webhook.

    Returns (success, error_message). error_message is None on success.
    """
    import requests

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        return True, None
    except requests.RequestException as exc:
        logger.warning("Slack webhook delivery failed: %s", exc)
        return False, str(exc)


def write_alert_log(spikes: list[dict], notified: bool, error: str | None, spark: "SparkSession") -> int:
    """Append one dq_alert_log row per spike. Returns the number of rows written."""
    if not spikes:
        return 0

    ensure_alert_log_table(spark)

    from pyspark.sql.types import (
        BooleanType, DoubleType, StringType, StructField, StructType, TimestampType,
    )

    now = datetime.now(tz=timezone.utc)
    schema = StructType([
        StructField("alert_id",        StringType(),    False),
        StructField("rule_name",       StringType(),    True),
        StructField("run_config_name", StringType(),    True),
        StructField("run_id",          StringType(),    True),
        StructField("baseline_pct",    DoubleType(),    True),
        StructField("last_run_pct",    DoubleType(),    True),
        StructField("multiplier_used", DoubleType(),    True),
        StructField("triggered_at",    TimestampType(), True),
        StructField("notified",        BooleanType(),   True),
        StructField("notify_error",    StringType(),    True),
    ])

    rows = [
        (
            str(uuid.uuid4()), s["rule_name"], s["run_config_name"], s["run_id"],
            s["baseline_pct"], s["last_run_pct"], s["multiplier_used"],
            now, notified, error,
        )
        for s in spikes
    ]

    (
        spark.createDataFrame(rows, schema=schema)
        .write.format("delta").mode("append")
        .saveAsTable(TABLE_ALERT_LOG)
    )

    logger.info("Wrote %d alert log row(s) to %s", len(rows), TABLE_ALERT_LOG)
    return len(rows)


def run_alert_check(
    spark: "SparkSession",
    run_config_name: str,
    webhook_url: str,
    lookback_runs: int = ALERT_LOOKBACK_RUNS,
) -> list[dict]:
    """
    Orchestrates a full alert check: detect spikes, notify Slack, log results.

    Returns the list of spikes processed (empty if none found).
    """
    spikes = detect_spikes(spark, run_config_name, lookback_runs)
    if not spikes:
        logger.info("No spikes detected for run_config_name=%s", run_config_name)
        return []

    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — logging spikes without notifying")
        write_alert_log(spikes, notified=False, error="webhook_url not configured", spark=spark)
        return spikes

    payload = format_slack_message(spikes)
    success, error = send_slack_alert(payload, webhook_url)
    write_alert_log(spikes, notified=success, error=error, spark=spark)

    return spikes
