"""
LLM rule suggester — builds a structured prompt from a DataFrame profile,
sends it to an LLM backend, and parses the response into DQX rule dicts.
"""

from __future__ import annotations

import json
import re

from ai_suggester.llm_backend import get_backend, LLMBackend
from src.logger import get_logger

logger = get_logger(__name__)

_REQUIRED_KEYS = {"name", "column", "function", "constraint", "criticality", "message", "reasoning"}

_SYSTEM_PROMPT = """You are a senior data quality engineer specialising in retail and e-commerce datasets.
Your task is to suggest data quality rules that cannot be derived from statistics alone.

STRICT OUTPUT FORMAT:
- Output ONLY a valid JSON array of rule objects.
- No prose, no markdown code fences, no explanations outside the JSON.
- Each object must have exactly these keys:
  name, column, function, constraint, criticality, message, reasoning

RULES FOR EACH KEY:
- name: snake_case string, unique, descriptive (e.g. "order_date_not_future")
- column: the primary column this rule checks (use null/empty string for cross-column rules)
- function: one of "is_not_null", "min_value", "max_value", "sql_expression", "matches_regex", "is_in_list"
- constraint: a JSON object appropriate for the function:
    is_not_null   → {}
    min_value     → {"min": <number>}
    max_value     → {"max": <number>}
    sql_expression → {"expression": "<SQL boolean expression>"}
    matches_regex → {"regex": "<pattern>"}
    is_in_list    → {"allowed": [<values>]}
- criticality: "error" (data must be rejected) or "warning" (flag for review)
- message: clear human-readable description shown in quarantine reports
- reasoning: why this rule matters for the domain (1-2 sentences)
"""


def suggest_llm_rules(
    profile: dict,
    run_config_name: str,
    context: str,
    stat_rules: list[dict] | None = None,
    backend: str | None = None,
    llm: LLMBackend | None = None,
) -> list[dict]:
    """
    Send the column profile to an LLM and return a list of suggested DQX rules.

    profile          — output of profiler.profile_dataframe()
    run_config_name  — e.g. "batch_sales_rules"
    context          — plain-English description of the dataset domain
    stat_rules       — already-generated statistical rules (avoids duplicates)
    backend          — "claude" | "azure_openai" | None (reads env var)
    llm              — pre-constructed LLMBackend; skips get_backend() if provided
    """
    if llm is None:
        llm = get_backend(backend)

    prompt = _build_prompt(profile, context, stat_rules or [])
    logger.info("Sending profiling prompt to LLM (%d columns)", len(profile))

    raw_response = llm.generate(prompt=prompt, system=_SYSTEM_PROMPT)
    logger.debug("LLM raw response:\n%s", raw_response[:500])

    rules = _parse_response(raw_response, run_config_name, _backend_name(llm))
    logger.info("LLM returned %d valid rule suggestions", len(rules))
    return rules


def _build_prompt(profile: dict, context: str, stat_rules: list[dict]) -> str:
    lines: list[str] = [
        f"Dataset context: {context}",
        "",
        "Column profiles:",
    ]

    for col_name, stats in profile.items():
        dtype = stats.get("dtype", "unknown")
        null_pct = stats.get("null_pct", 0.0)
        distinct = stats.get("distinct_count", 0)
        samples = stats.get("sample_values", [])
        dist_vals = stats.get("distinct_values")

        line = (
            f"  {col_name} ({dtype}): "
            f"null={null_pct * 100:.1f}%, "
            f"distinct={distinct}"
        )
        if dist_vals:
            line += f", values={dist_vals}"
        if stats.get("min") is not None:
            line += f", min={stats['min']}, max={stats['max']}, mean={stats.get('mean')}"
        if samples:
            line += f", samples={samples}"
        lines.append(line)

    if stat_rules:
        lines += [
            "",
            "Statistical rules already generated (do NOT duplicate these):",
        ]
        for r in stat_rules:
            lines.append(f"  - {r['name']}: {r.get('message', '')}")

    lines += [
        "",
        "Suggest business-logic data quality rules that the statistics above cannot detect.",
        "Focus on:",
        "  1. Cross-column integrity (e.g. if column A is X then column B must be Y)",
        "  2. Domain constraints specific to retail/e-commerce",
        "  3. Temporal or referential anomalies",
        "",
        "Output ONLY a JSON array. No prose.",
    ]
    return "\n".join(lines)


def _parse_response(raw: str, run_config_name: str, backend_name: str) -> list[dict]:
    """Extract and validate JSON rules from the LLM response string."""
    # Strip markdown code fences if the model added them despite instructions
    cleaned = re.sub(r"```(?:json)?", "", raw).strip()

    # Find the outermost JSON array
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

        rule["source"] = "llm"
        rule["llm_backend"] = backend_name
        rule["confidence"] = None
        valid.append(rule)

    return valid


def _backend_name(llm: LLMBackend) -> str:
    return type(llm).__name__.replace("Backend", "").lower()
