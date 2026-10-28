"""
One suggestion cycle for the continuous streaming flow.

Composes the existing quarantine analyzer, statistical generator and LLM rule
suggester against the *streaming* tables, stages only new suggestions (bounded
per cycle) and records a notification row so reviewers know there is work.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from ai_suggester.profiler import profile_dataframe
from ai_suggester.quarantine_analyzer import extract_violation_patterns, suggest_rules_from_quarantine
from ai_suggester.llm_rule_suggester import suggest_llm_rules
from ai_suggester.rule_generator import suggest_statistical_rules
from ai_suggester.rule_writer import select_new_suggestions, write_suggestions_to_staging
from config.constants import TABLE_RULES_STAGING, TABLE_REVIEW_NOTIFICATIONS
from src.logger import get_logger

logger = get_logger(__name__)

_NOTIFICATIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE_REVIEW_NOTIFICATIONS} (
    cycle_id         STRING,
    created_at       TIMESTAMP,
    run_config_name  STRING,
    new_count        INT,
    pending_total    INT,
    rule_names       ARRAY<STRING>,
    how_to_review    STRING
) USING DELTA
"""

HOW_TO_REVIEW = (
    "Jobs > [RTM] Review Rule > Run now with different parameters: "
    "action=approve|reject, rule=<rule name>"
)


def should_run_cycle(
    current_count: int,
    last_count: int | None,
    min_rows: int,
    min_new_rows: int,
) -> bool:
    """
    First cycle: wait until the quarantine table has min_rows. Later cycles:
    run only if at least min_new_rows new quarantined rows arrived since the
    last cycle (keeps LLM calls and suggestion noise down).
    """
    if last_count is None:
        return current_count >= min_rows
    return current_count - last_count >= min_new_rows


def build_notification(
    cycle_id: str,
    run_config_name: str,
    new_rule_names: list[str],
    pending_total: int,
) -> dict:
    """The row recorded (and logged) when a cycle stages new suggestions."""
    return {
        "cycle_id": cycle_id,
        "run_config_name": run_config_name,
        "new_count": len(new_rule_names),
        "pending_total": pending_total,
        "rule_names": list(new_rule_names),
        "how_to_review": HOW_TO_REVIEW,
    }


def format_notification(n: dict) -> str:
    return (
        f"{n['new_count']} new rule suggestion(s) pending review for {n['run_config_name']} "
        f"({n['pending_total']} pending in total): {', '.join(n['rule_names'])}. "
        f"To review: {n['how_to_review']}"
    )


def _safe(label: str, fn, default):
    """Run one cycle step; log and swallow any error so the loop keeps going."""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001 - LLM/network flakiness must not stop the loop
        logger.warning("%s failed, continuing: %s", label, str(exc).splitlines()[0] if str(exc) else exc)
        return default


def run_cycle(
    spark,
    *,
    quarantine_table: str,
    profile_table: str,
    run_config_name: str,
    context: str,
    llm_backend: str,
    max_new: int,
    top_n: int = 20,
) -> dict | None:
    """
    Run one suggestion cycle. Returns the notification dict if new suggestions
    were staged, otherwise None.
    """
    cycle_id = str(uuid.uuid4())

    quarantine_rules = _safe(
        "Quarantine analysis",
        lambda: suggest_rules_from_quarantine(
            patterns=extract_violation_patterns(spark.table(quarantine_table), top_n=top_n),
            run_config_name=run_config_name,
            context=context,
            backend=llm_backend,
        ),
        [],
    )

    profile = _safe("Profiling", lambda: profile_dataframe(spark.table(profile_table)), {})
    stat_rules = _safe("Statistical rules", lambda: suggest_statistical_rules(profile, run_config_name), []) if profile else []
    llm_rules = _safe(
        "LLM rule suggestions",
        lambda: suggest_llm_rules(
            profile=profile,
            run_config_name=run_config_name,
            context=context,
            stat_rules=stat_rules,
            backend=llm_backend,
        ),
        [],
    ) if profile else []

    # Failure-driven suggestions first, then LLM, then statistical baseline
    candidates = quarantine_rules + llm_rules + stat_rules
    new_rules = select_new_suggestions(candidates, run_config_name, spark, max_new=max_new)
    if not new_rules:
        logger.info("Cycle %s: no new suggestions (candidates=%d)", cycle_id[:8], len(candidates))
        return None

    written = write_suggestions_to_staging(new_rules, run_config_name, spark)
    pending_total = (
        spark.table(TABLE_RULES_STAGING)
        .filter(f"run_config_name = '{run_config_name}' AND status = 'pending'")
        .count()
    )
    notification = build_notification(cycle_id, run_config_name, [r["name"] for r in new_rules], pending_total)
    _record_notification(spark, notification)
    logger.info("Cycle %s staged %d suggestion(s). %s", cycle_id[:8], written, format_notification(notification))
    return notification


def _record_notification(spark, n: dict) -> None:
    from pyspark.sql.types import (
        ArrayType, IntegerType, StringType, StructField, StructType, TimestampType,
    )

    spark.sql(_NOTIFICATIONS_DDL)
    schema = StructType([
        StructField("cycle_id",        StringType(),            False),
        StructField("created_at",      TimestampType(),         True),
        StructField("run_config_name", StringType(),            True),
        StructField("new_count",       IntegerType(),           True),
        StructField("pending_total",   IntegerType(),           True),
        StructField("rule_names",      ArrayType(StringType()), True),
        StructField("how_to_review",   StringType(),            True),
    ])
    row = (
        n["cycle_id"], datetime.now(tz=timezone.utc), n["run_config_name"],
        n["new_count"], n["pending_total"], n["rule_names"], n["how_to_review"],
    )
    (
        spark.createDataFrame([row], schema=schema)
        .write.format("delta").mode("append").saveAsTable(TABLE_REVIEW_NOTIFICATIONS)
    )
