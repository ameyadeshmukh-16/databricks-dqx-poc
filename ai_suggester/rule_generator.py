"""
Statistical rule generator — derives DQX rule suggestions purely from
DataFrame profile statistics, with no LLM call.
"""

from __future__ import annotations

from config.constants import (
    STAT_NULL_THRESHOLD,
    STAT_DISTINCT_THRESHOLD,
    STAT_COVERAGE_THRESHOLD,
)
from src.logger import get_logger

logger = get_logger(__name__)

_NUMERIC_TYPES = {
    "ByteType", "ShortType", "IntegerType", "LongType",
    "FloatType", "DoubleType", "DecimalType",
}

_INTEGRAL_TYPES = {"ByteType", "ShortType", "IntegerType", "LongType"}

# Name tokens that mark an integer column as an identifier / categorical code
# (e.g. division=11): range checks on these are meaningless.
_CODE_NAME_TOKENS = {"id", "code", "division", "key", "zip", "sku", "number", "no"}


def _is_code_like(col_name: str, dtype: str) -> bool:
    """True for integer columns whose name says they hold a code/identifier."""
    if dtype not in _INTEGRAL_TYPES:
        return False
    return any(tok in _CODE_NAME_TOKENS for tok in col_name.lower().split("_"))


def suggest_statistical_rules(profile: dict, run_config_name: str) -> list[dict]:
    """
    Analyse a column profile dict and return a list of suggested DQX rules.

    Each rule dict contains:
        name, column, function, constraint, criticality, message,
        source ("statistical"), confidence (0.0–1.0), reasoning
    """
    rules: list[dict] = []

    for col_name, stats in profile.items():
        dtype = stats.get("dtype", "")
        null_pct = stats.get("null_pct", 1.0)
        distinct_count = stats.get("distinct_count", 0)
        distinct_values = stats.get("distinct_values")  # None when not collected
        is_numeric = dtype in _NUMERIC_TYPES

        # --- Rule 1: is_not_null ---
        if null_pct < STAT_NULL_THRESHOLD:
            confidence = round(1.0 - null_pct, 4)
            rules.append({
                "name": f"{col_name}_not_null",
                "column": col_name,
                "function": "is_not_null",
                "constraint": {},
                "criticality": "error",
                "message": f"{col_name} must not be null",
                "source": "statistical",
                "confidence": confidence,
                "reasoning": (
                    f"{null_pct * 100:.2f}% null rate observed — well below the "
                    f"{STAT_NULL_THRESHOLD * 100:.0f}% threshold"
                ),
            })

        # --- Rule 2: is_in_list (for low-cardinality string/categorical columns) ---
        if (
            distinct_values is not None
            and distinct_count <= STAT_DISTINCT_THRESHOLD
            and distinct_count >= 2
            and not is_numeric
        ):
            non_null_share = 1.0 - null_pct
            if non_null_share >= STAT_COVERAGE_THRESHOLD:
                quoted = [f"'{v}'" for v in distinct_values]
                constraint_expr = f"{col_name} IN ({', '.join(quoted)})"
                rules.append({
                    "name": f"{col_name}_valid_values",
                    "column": col_name,
                    "function": "sql_expression",
                    "constraint": {"expression": constraint_expr},
                    "criticality": "warning",
                    "message": (
                        f"{col_name} should be one of: {', '.join(distinct_values)}"
                    ),
                    "source": "statistical",
                    "confidence": round(non_null_share, 4),
                    "reasoning": (
                        f"Only {distinct_count} distinct values observed across "
                        f"{non_null_share * 100:.1f}% of non-null rows"
                    ),
                })

        # --- Rule 3: min_value (numeric columns that are always positive) ---
        # Code-like integers (division, store_no, ...) are categorical: skip ranges.
        if is_numeric and not _is_code_like(col_name, dtype):
            col_min = stats.get("min")
            col_max = stats.get("max")

            if col_min is not None and col_min > 0:
                rules.append({
                    "name": f"{col_name}_min_value",
                    "column": col_name,
                    "function": "min_value",
                    "constraint": {"min": 0},
                    "criticality": "error",
                    "message": f"{col_name} must be greater than 0",
                    "source": "statistical",
                    "confidence": 0.9,
                    "reasoning": (
                        f"Observed minimum value is {col_min} — all sampled values "
                        "are strictly positive"
                    ),
                })

            # --- Rule 4: max_value (sanity ceiling) ---
            if col_max is not None and col_max > 0:
                ceiling = _round_up_ceiling(col_max)
                rules.append({
                    "name": f"{col_name}_max_value",
                    "column": col_name,
                    "function": "max_value",
                    "constraint": {"max": ceiling},
                    "criticality": "warning",
                    "message": f"{col_name} should not exceed {ceiling}",
                    "source": "statistical",
                    "confidence": 0.75,
                    "reasoning": (
                        f"Observed maximum value is {col_max}; ceiling set to "
                        f"{ceiling} (10× headroom)"
                    ),
                })

    logger.info(
        "Statistical rule generator produced %d rule suggestions for '%s'",
        len(rules), run_config_name,
    )
    return rules


def _round_up_ceiling(value: float) -> float:
    """Return a round ceiling ~10× the observed maximum, useful for sanity checks."""
    import math
    if value <= 0:
        return value
    magnitude = 10 ** math.floor(math.log10(value))
    return math.ceil(value / magnitude) * magnitude * 10
