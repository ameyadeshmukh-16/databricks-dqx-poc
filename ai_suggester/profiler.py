"""
DataFrame profiler — computes per-column statistics used by both the
statistical rule generator and the LLM prompt builder.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

from config.constants import PROFILER_SAMPLE_SIZE, STAT_DISTINCT_THRESHOLD
from src.logger import get_logger

logger = get_logger(__name__)

_NUMERIC_TYPES = {
    "ByteType", "ShortType", "IntegerType", "LongType",
    "FloatType", "DoubleType", "DecimalType",
}

_COMPLEX_TYPES = {"MapType", "ArrayType", "StructType"}


def profile_dataframe(df: "DataFrame", sample_size: int = PROFILER_SAMPLE_SIZE) -> dict:
    """
    Profile a Spark DataFrame and return per-column statistics.

    Returns a dict keyed by column name.  Each value is a dict with:
        dtype           str   — Spark type name (e.g. "StringType")
        null_pct        float — fraction of null rows  (0.0 – 1.0)
        distinct_count  int   — number of distinct non-null values
        distinct_values list  — only present when distinct_count ≤ STAT_DISTINCT_THRESHOLD
        min             float — numeric columns only
        max             float — numeric columns only
        mean            float — numeric columns only
        stddev          float — numeric columns only
        sample_values   list  — up to 5 random non-null values (all types)
    """
    from pyspark.sql import functions as F

    total_rows = df.count()
    if total_rows == 0:
        logger.warning("DataFrame is empty — profiling skipped")
        return {}

    # Sample for large DataFrames to keep compute tractable
    if total_rows > sample_size:
        fraction = sample_size / total_rows
        df = df.sample(fraction=fraction, seed=42)
        sample_rows = df.count()
        logger.info("Sampled %d rows from %d total (fraction=%.4f)", sample_rows, total_rows, fraction)
    else:
        sample_rows = total_rows
        logger.info("Profiling %d rows (full scan — below sample threshold)", sample_rows)

    profile: dict = {}

    for field in df.schema.fields:
        col_name = field.name

        # Skip DQX-internal metadata columns (_errors, _warnings) — complex nested types,
        # not business data; rule generation against them is meaningless
        if col_name.startswith("_"):
            logger.debug("  %s: skipped (DQX metadata column)", col_name)
            continue

        dtype_name = type(field.dataType).__name__
        is_numeric = dtype_name in _NUMERIC_TYPES
        is_complex = dtype_name in _COMPLEX_TYPES

        col = F.col(col_name)

        # Core aggregations run in a single Spark pass
        agg_exprs = [
            F.count(F.when(col.isNull(), 1)).alias("null_count"),
        ]
        if not is_complex:
            agg_exprs.append(F.countDistinct(col).alias("distinct_count"))
        if is_numeric:
            agg_exprs += [
                F.min(col).cast("double").alias("min_val"),
                F.max(col).cast("double").alias("max_val"),
                F.mean(col).alias("mean_val"),
                F.stddev(col).alias("stddev_val"),
            ]

        agg_row = df.agg(*agg_exprs).collect()[0]

        null_count = agg_row["null_count"] or 0
        null_pct = null_count / sample_rows if sample_rows > 0 else 0.0
        distinct_count = (agg_row["distinct_count"] if not is_complex else 0) or 0

        entry: dict = {
            "dtype": dtype_name,
            "null_pct": round(null_pct, 4),
            "distinct_count": distinct_count,
        }

        if not is_complex and distinct_count <= STAT_DISTINCT_THRESHOLD:
            distinct_vals = (
                df.select(col_name)
                .where(col.isNotNull())
                .distinct()
                .limit(STAT_DISTINCT_THRESHOLD)
                .rdd.flatMap(lambda r: r)
                .collect()
            )
            entry["distinct_values"] = [str(v) for v in distinct_vals]

        if is_numeric:
            entry["min"] = agg_row["min_val"]
            entry["max"] = agg_row["max_val"]
            entry["mean"] = round(agg_row["mean_val"] or 0.0, 4)
            entry["stddev"] = round(agg_row["stddev_val"] or 0.0, 4)

        # Grab up to 5 non-null sample values (skip for complex types — rdd.flatMap fails on maps)
        if not is_complex:
            sample_vals = (
                df.select(col_name)
                .where(col.isNotNull())
                .limit(5)
                .rdd.flatMap(lambda r: r)
                .collect()
            )
            entry["sample_values"] = [str(v) for v in sample_vals]
        else:
            entry["sample_values"] = []

        profile[col_name] = entry
        logger.debug(
            "  %s: dtype=%s null_pct=%.2f%% distinct=%d",
            col_name, dtype_name, null_pct * 100, distinct_count,
        )

    logger.info("Profiling complete — %d columns", len(profile))
    return profile
