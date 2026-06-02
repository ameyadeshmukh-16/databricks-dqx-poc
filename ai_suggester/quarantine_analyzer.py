"""
Quarantine Analyzer — mines DQX quarantine tables for recurring violation
patterns and feeds them to an LLM to suggest new data quality rules.

The feedback loop:
  quarantine table  →  extract_violation_patterns()
                    →  suggest_rules_from_quarantine()
                    →  write_suggestions_to_staging()   (rule_writer.py)
                    →  dq_rules_staging  →  human review  →  dq_rules_registry
"""

from __future__ import annotations

import json
import re

from ai_suggester.llm_backend import get_backend
from ai_suggester.llm_rule_suggester import _backend_name
from src.logger import get_logger

logger = get_logger(__name__)

_SYSTEM_PROMPT = (
    "You are a data quality expert. Output ONLY a valid JSON array of rule objects. "
    "No prose, no markdown, no explanation outside the JSON."
)

_REQUIRED_KEYS = {"name", "column", "function", "constraint", "criticality", "message", "reasoning"}


def extract_violation_patterns(quarantine_df, top_n: int = 20) -> dict:
    """
    Analyse a DQX quarantine DataFrame and return a structured summary of the
    most frequent violation patterns.

    quarantine_df  — Spark DataFrame from a DQX quarantine table; must contain
                     an _errors column of type ARRAY<STRUCT<name, message, ...>>
    top_n          — number of top violation patterns to surface

    Returns a dict:
      {
        "total_quarantined": int,
        "violation_counts": [
          {
            "rule_name":               str,
            "message":                 str,
            "function":                str,
            "count":                   int,
            "pct_of_quarantine":       float,
            "sample_offending_values": {col: [val, ...], ...}
          },
          ...
        ]
      }
    """
    from pyspark.sql import functions as F

    total = quarantine_df.count()
    if total == 0:
        logger.info("Quarantine table is empty — no patterns to extract")
        return {"total_quarantined": 0, "violation_counts": []}

    business_cols = [c for c in quarantine_df.columns if not c.startswith("_")]
    logger.info(
        "Analysing %d quarantined records across %d business columns",
        total, len(business_cols),
    )

    # Explode the _errors array so each violation becomes its own row
    exploded = quarantine_df.select(
        *[F.col(c) for c in business_cols],
        F.explode(F.col("_errors")).alias("_err"),
    )

    violation_summary = (
        exploded
        .groupBy(
            F.col("_err.name").alias("rule_name"),
            F.col("_err.message").alias("message"),
            F.col("_err.function").alias("function"),
        )
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
        .limit(top_n)
        .collect()
    )

    patterns: dict = {"total_quarantined": total, "violation_counts": []}

    for row in violation_summary:
        # Sample offending business-column values for this specific rule (up to 4 cols)
        sample_rows = (
            exploded
            .filter(F.col("_err.name") == row["rule_name"])
            .select(business_cols[:4])
            .limit(5)
            .collect()
        )
        sample_values = {
            col: [str(r[col]) for r in sample_rows if r[col] is not None]
            for col in business_cols[:4]
        }

        patterns["violation_counts"].append({
            "rule_name":               row["rule_name"],
            "message":                 row["message"],
            "function":                row["function"],
            "count":                   row["count"],
            "pct_of_quarantine":       round(row["count"] / total * 100, 1),
            "sample_offending_values": sample_values,
        })

    logger.info("Extracted %d violation patterns", len(patterns["violation_counts"]))
    return patterns


def suggest_rules_from_quarantine(
    patterns: dict,
    run_config_name: str,
    context: str,
    backend: str | None = None,
) -> list[dict]:
    """
    Build an LLM prompt from quarantine violation patterns and return a list
    of new DQX rule suggestions.

    patterns         — output of extract_violation_patterns()
    run_config_name  — e.g. "sales_batch_rules" or "streaming_event_rules"
    context          — plain-English description of the dataset domain
    backend          — "claude" | "azure_openai" | "openrouter" | None (env var)

    Returns rule dicts in the same format as rule_generator / llm_rule_suggester,
    with source="quarantine_analysis".
    """
    if patterns.get("total_quarantined", 0) == 0:
        logger.info("No quarantine records — skipping LLM call")
        return []

    llm = get_backend(backend)
    prompt = _build_prompt(patterns, context)
    logger.info(
        "Sending quarantine pattern prompt to LLM (%d patterns)",
        len(patterns["violation_counts"]),
    )

    raw = llm.generate(prompt=prompt, system=_SYSTEM_PROMPT)
    logger.debug("LLM raw response:\n%s", raw[:500])

    rules = _parse_response(raw, run_config_name, _backend_name(llm))
    logger.info("LLM returned %d quarantine-derived rule suggestions", len(rules))
    return rules


def _build_prompt(patterns: dict, context: str) -> str:
    lines = [
        f"Dataset: {context}",
        f"\nTotal quarantined records: {patterns['total_quarantined']}",
        "\nTop violation patterns observed in the quarantine table:",
    ]

    for i, v in enumerate(patterns["violation_counts"], 1):
        lines.append(
            f"\n{i}. Rule fired: {v['rule_name']} | "
            f"Count: {v['count']} ({v['pct_of_quarantine']}% of quarantined records)"
        )
        lines.append(f"   Existing rule message: {v['message']}")
        for col, vals in list(v["sample_offending_values"].items())[:4]:
            if vals:
                lines.append(f"   Sample {col} values when this rule fired: {vals}")

    lines += [
        "\n\nBased on these patterns, suggest NEW data quality rules that:",
        "1. Catch additional violation sub-patterns not fully covered by existing rules",
        "2. Detect cross-column correlations that indicate bad data",
        "3. Identify stricter thresholds or allowlists warranted by the observed data",
        "\nDo NOT duplicate the existing rules listed above.",
        "\nOutput a JSON array. Each rule object must have exactly these keys:",
        '  name (snake_case), column (or "" for cross-column), function',
        "  (is_not_null | min_value | max_value | sql_expression | matches_regex | is_in_list),",
        "  constraint (object matching the function), criticality (error | warning),",
        "  message (human-readable, shown in quarantine reports), reasoning (1-2 sentences)",
    ]
    return "\n".join(lines)


def _parse_response(raw: str, run_config_name: str, backend_name: str) -> list[dict]:
    cleaned = re.sub(r"```(?:json)?", "", raw).strip()
    match = re.search(r"\[.*\]", cleaned, re.DOTALL)
    if not match:
        logger.warning("LLM response contained no JSON array — returning empty list")
        return []

    try:
        candidates: list = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        logger.warning("Failed to parse LLM JSON response: %s", exc)
        return []

    valid: list[dict] = []
    for i, rule in enumerate(candidates):
        if not isinstance(rule, dict):
            logger.warning("Rule %d is not a dict — skipped", i)
            continue
        missing = _REQUIRED_KEYS - rule.keys()
        if missing:
            logger.warning("Rule %d missing keys %s — skipped", i, missing)
            continue
        if rule.get("criticality") not in ("error", "warning"):
            rule["criticality"] = "warning"
        rule["source"] = "quarantine_analysis"
        rule["llm_backend"] = backend_name
        rule["confidence"] = None
        valid.append(rule)

    return valid
