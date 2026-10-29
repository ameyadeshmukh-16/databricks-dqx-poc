"""
Plain-English descriptions for the demo's tables and columns, applied as Unity Catalog comments so
Catalog Explorer (and lineage) read well in front of an audience. Idempotent: running it again just
rewrites the same comments, and tables or columns that do not exist (yet) are skipped.
"""

from __future__ import annotations

SCHEMA_COMMENT = (
    "Real-time data quality demo: retail point-of-sale events validated by DQX rules, "
    "with AI-suggested rules that a human approves before they are enforced."
)

_DQX_ERRORS = "DQX result: the rules this event failed (rule name, message, columns); an event with any entry is quarantined."
_DQX_WARNINGS = "DQX result: warning-level rules this event broke; the event is kept but flagged."

TABLE_DOCS: dict[str, dict] = {
    # ---- streaming events ----------------------------------------------------------------------
    "stream_events_clean": {
        "comment": "Point-of-sale events that passed every error-level rule. Written by the streaming pipeline.",
        "columns": {
            "event_id": "Unique id of the sales event.",
            "store_id": "Store that produced the event.",
            "product_id": "Product sold.",
            "division": "Business division code (011, 012, 013 are the approved codes).",
            "quantity": "Units sold; must be at least 1.",
            "unit_price": "Price per unit; must be greater than 0.",
            "coupon_applied": "Whether a coupon was used for the sale.",
            "event_timestamp": "When the sale happened, as sent by the source system.",
            "ingested_at": "When the pipeline received the event.",
            "_warnings": _DQX_WARNINGS,
        },
    },
    "stream_events_quarantine": {
        "comment": "Events that broke at least one error-level rule, kept with the reason (_errors) instead of being dropped.",
        "columns": {
            "event_id": "Unique id of the sales event.",
            "store_id": "Store that produced the event.",
            "product_id": "Product sold.",
            "division": "Business division code as received (may be missing or unapproved).",
            "quantity": "Units sold as received (may be invalid, e.g. below 1).",
            "unit_price": "Price per unit as received (may be invalid, e.g. 0).",
            "coupon_applied": "Whether a coupon was used for the sale.",
            "event_timestamp": "When the sale happened, as sent by the source system.",
            "ingested_at": "When the pipeline received the event.",
            "_errors": _DQX_ERRORS,
            "_warnings": _DQX_WARNINGS,
        },
    },
    "stream_batch_metrics": {
        "comment": "One row per streaming micro-batch: how many events were clean and how many were quarantined.",
        "columns": {
            "batch_id": "Sequence number of the micro-batch.",
            "clean_count": "Events in the batch that passed all error-level rules.",
            "quarantine_count": "Events in the batch sent to quarantine.",
            "total_count": "All events in the batch.",
            "processed_at": "When the batch was processed.",
        },
    },
    "stream_consumer_ready": {
        "comment": "Handshake: written once the streaming consumer is listening, so the producer only sends after that.",
        "columns": {"started_at": "When the consumer started listening.", "run_id": "Identifier of the streaming run."},
    },
    # ---- rules and governance ------------------------------------------------------------------
    "dq_rules_registry": {
        "comment": "The live rules enforced by the pipelines (DQX format). A rule only gets here after seeding or a human approval.",
        "columns": {
            "name": "Rule name.",
            "criticality": "error = event is quarantined; warn = event is flagged but kept.",
            "check": "The DQX check: function, columns and arguments.",
            "filter": "Optional condition limiting which events the rule applies to.",
            "run_config_name": "Rule group, e.g. streaming_event_rules or sales_batch_rules.",
            "user_metadata": "Free-form metadata, such as the human-readable message.",
            "created_at": "When the rule became active.",
            "rule_fingerprint": "DQX hash of the rule (empty for rules approved from suggestions).",
            "rule_set_fingerprint": "DQX hash of the whole rule set (empty for rules approved from suggestions).",
        },
    },
    "dq_rules_staging": {
        "comment": "Suggested rules waiting for, or showing the outcome of, a human decision: the audit trail of AI-suggested rules.",
        "columns": {
            "rule_id": "Unique id of the suggestion.",
            "name": "Suggested rule name.",
            "column_name": "Column the rule checks.",
            "function": "Kind of check (e.g. is_not_null, min_value, matches_regex).",
            "constraint": "The check's parameters, as JSON.",
            "criticality": "How serious a violation is: error or warning.",
            "message": "Human-readable description of the rule.",
            "run_config_name": "Rule group the suggestion is for.",
            "source": "Where it came from: statistical analysis, quarantine analysis or the AI model.",
            "llm_backend": "The AI model that produced the suggestion (empty for statistical rules).",
            "confidence": "Confidence score for statistical suggestions (0 to 1).",
            "reasoning": "Why the suggestion was made.",
            "status": "pending, approved or rejected.",
            "suggested_at": "When the suggestion was created.",
            "reviewed_at": "When a person approved or rejected it.",
            "reviewed_by": "Who approved or rejected it.",
        },
    },
    "dq_rule_metrics": {
        "comment": "How many events each rule caught in each run or micro-batch (error-level rules only).",
        "columns": {
            "run_id": "Identifier of the batch or run.",
            "run_type": "streaming or batch.",
            "run_config_name": "Rule group.",
            "rule_name": "Rule that caught events.",
            "function": "Kind of check.",
            "violations": "Events the rule caught in this run.",
            "total_records": "Events checked in this run.",
            "violation_pct": "violations as a percentage of total_records.",
            "recorded_at": "When the metric was written.",
        },
    },
    "dq_review_notifications": {
        "comment": "One row each time new AI rule suggestions land and need a review.",
        "columns": {
            "cycle_id": "Identifier of the suggestion cycle.",
            "created_at": "When the suggestions were staged.",
            "run_config_name": "Rule group.",
            "new_count": "New suggestions in this cycle.",
            "pending_total": "Suggestions waiting for a decision after this cycle.",
            "rule_names": "Names of the new suggestions.",
            "how_to_review": "How a reviewer can act on them.",
        },
    },
    "dq_alert_log": {
        "comment": "Violation-rate spikes detected against each rule's own baseline, and whether the alert was delivered.",
        "columns": {
            "alert_id": "Unique id of the alert.",
            "rule_name": "Rule whose violation rate spiked.",
            "run_config_name": "Rule group.",
            "run_id": "Run in which the spike was seen.",
            "baseline_pct": "Average violation rate over earlier runs.",
            "last_run_pct": "Violation rate in the run that spiked.",
            "multiplier_used": "Spike threshold factor applied to the baseline.",
            "triggered_at": "When the alert was raised.",
            "notified": "Whether the notification was delivered.",
            "notify_error": "Why delivery failed, if it did.",
        },
    },
    # ---- batch pipeline ------------------------------------------------------------------------
    "sales_staging": {
        "comment": "Raw sales transactions loaded from the CSV, before validation (batch pipeline).",
        "columns": {},
    },
    "sales_clean": {"comment": "Sales transactions that passed validation (batch pipeline).", "columns": {}},
    "sales_quarantine": {
        "comment": "Sales transactions that failed validation, with the reasons (batch pipeline).",
        "columns": {"_errors": _DQX_ERRORS, "_warnings": _DQX_WARNINGS},
    },
}

# Tables the streaming pipeline creates; the document_tables task waits for these.
STREAMING_TABLES = (
    "dq_rules_registry", "stream_events_clean", "stream_events_quarantine", "stream_batch_metrics",
    "stream_consumer_ready", "dq_rule_metrics", "dq_rules_staging", "dq_review_notifications",
)


# Tables the running stream writes to. Changing their metadata while the stream creates and first writes
# them makes its write conflict, so they are documented only after the stream has finished a few batches.
STREAM_OWNED = (
    "stream_events_clean", "stream_events_quarantine", "stream_batch_metrics", "stream_consumer_ready",
    "dq_rule_metrics",
)
MIN_STREAM_BATCHES = 3


def docs_to_apply(done: set, batches_written: int, min_batches: int = MIN_STREAM_BATCHES,
                  docs: dict | None = None) -> dict:
    """The comment catalogue entries still to apply now: not already done, and stream-owned tables only once the stream is settled."""
    docs = TABLE_DOCS if docs is None else docs
    settled = batches_written >= min_batches
    return {name: doc for name, doc in docs.items()
            if name not in done and (settled or name not in STREAM_OWNED)}


def sql_literal(text: str) -> str:
    """Single-quoted Spark SQL string literal (backslashes and quotes escaped)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def table_statements(fqn: str, doc: dict, existing_columns: list[str]) -> list[tuple[str, str]]:
    """(label, statement) pairs for one table, covering only columns that really exist."""
    statements = [(fqn, f"COMMENT ON TABLE {fqn} IS {sql_literal(doc['comment'])}")]
    present = {c.lower(): c for c in existing_columns}
    for column, text in doc.get("columns", {}).items():
        real = present.get(column.lower())
        if real:
            statements.append((f"{fqn}.{real}", f"ALTER TABLE {fqn} ALTER COLUMN `{real}` COMMENT {sql_literal(text)}"))
    return statements


def apply_table_docs(spark, catalog: str, schema: str, docs: dict | None = None) -> dict:
    """
    Apply every comment that can be applied now. Returns
    {"documented": [tables], "missing": [tables not created yet], "failed": [(label, error)]}.
    """
    docs = TABLE_DOCS if docs is None else docs
    result = {"documented": [], "missing": [], "failed": []}
    if not spark.catalog.databaseExists(f"{catalog}.{schema}"):
        result["missing"] = list(docs)
        return result

    try:
        spark.sql(f"COMMENT ON SCHEMA {catalog}.{schema} IS {sql_literal(SCHEMA_COMMENT)}")
    except Exception as exc:                                    # noqa: BLE001 - reported, not fatal
        result["failed"].append((f"{catalog}.{schema}", str(exc).splitlines()[0][:160]))

    for name, doc in docs.items():
        fqn = f"{catalog}.{schema}.{name}"
        if not spark.catalog.tableExists(fqn):
            result["missing"].append(name)
            continue
        ok = True
        for label, statement in table_statements(fqn, doc, spark.table(fqn).columns):
            try:
                spark.sql(statement)
            except Exception as exc:                            # noqa: BLE001 - one bad column must not stop the rest
                ok = False
                result["failed"].append((label, str(exc).splitlines()[0][:160]))
        if ok:
            result["documented"].append(name)
    return result
