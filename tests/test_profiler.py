"""
Unit tests for ai_suggester.profiler.

Uses a local PySpark session so no Databricks connection is needed.
Run with: pytest tests/test_profiler.py -v
"""

import pytest

pytest.importorskip("pyspark", reason="PySpark not installed — skipping profiler tests")

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from ai_suggester.profiler import profile_dataframe
from config.constants import STAT_DISTINCT_THRESHOLD


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_profiler")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture()
def sample_df(spark):
    schema = StructType([
        StructField("event_id",  StringType(),  True),
        StructField("quantity",  IntegerType(), True),
        StructField("price",     DoubleType(),  True),
        StructField("division",  StringType(),  True),
    ])
    data = [
        ("EVT001", 2,    9.99,  "011"),
        ("EVT002", 5,    19.99, "012"),
        ("EVT003", None, 4.99,  "011"),
        ("EVT004", 1,    None,  "013"),
        ("EVT005", 3,    14.99, "011"),
    ]
    return spark.createDataFrame(data, schema=schema)


def test_profile_returns_all_columns(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    assert set(profile.keys()) == {"event_id", "quantity", "price", "division"}


def test_null_pct_correct(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    # quantity has 1 null out of 5 rows → 20%
    assert abs(profile["quantity"]["null_pct"] - 0.20) < 0.01
    # price has 1 null out of 5 rows → 20%
    assert abs(profile["price"]["null_pct"] - 0.20) < 0.01
    # event_id has 0 nulls
    assert profile["event_id"]["null_pct"] == 0.0


def test_numeric_stats_present(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    qty = profile["quantity"]
    assert "min" in qty
    assert "max" in qty
    assert "mean" in qty
    assert "stddev" in qty
    assert qty["min"] == 1.0
    assert qty["max"] == 5.0


def test_string_columns_have_no_numeric_stats(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    assert "min" not in profile["event_id"]
    assert "max" not in profile["event_id"]


def test_distinct_values_collected_for_low_cardinality(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    # division has 3 distinct values — well below STAT_DISTINCT_THRESHOLD
    assert "distinct_values" in profile["division"]
    assert set(profile["division"]["distinct_values"]) == {"011", "012", "013"}


def test_distinct_values_not_collected_for_high_cardinality(spark):
    # Create a DataFrame with more distinct values than the threshold
    data = [(str(i), i) for i in range(STAT_DISTINCT_THRESHOLD + 10)]
    schema = StructType([
        StructField("id",    StringType(),  True),
        StructField("value", IntegerType(), True),
    ])
    df = spark.createDataFrame(data, schema=schema)
    profile = profile_dataframe(df, sample_size=10_000)
    assert "distinct_values" not in profile["id"]


def test_sample_values_present(sample_df):
    profile = profile_dataframe(sample_df, sample_size=100)
    for col_name in profile:
        assert "sample_values" in profile[col_name]
        assert isinstance(profile[col_name]["sample_values"], list)


def test_empty_dataframe_returns_empty_profile(spark):
    schema = StructType([StructField("x", StringType(), True)])
    df = spark.createDataFrame([], schema=schema)
    profile = profile_dataframe(df, sample_size=100)
    assert profile == {}


def test_dqx_metadata_columns_skipped(spark):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("_errors",  StringType(), True),
        StructField("_warnings", StringType(), True),
    ])
    df = spark.createDataFrame([("EVT001", "[]", "[]")], schema=schema)
    profile = profile_dataframe(df, sample_size=100)
    assert "event_id" in profile
    assert "_errors" not in profile
    assert "_warnings" not in profile


def test_complex_type_columns_handled(spark):
    from pyspark.sql.types import MapType
    schema = StructType([
        StructField("name",     StringType(),                        True),
        StructField("metadata", MapType(StringType(), StringType()), True),
    ])
    df = spark.createDataFrame([("Alice", {"k": "v"})], schema=schema)
    profile = profile_dataframe(df, sample_size=100)
    assert "name" in profile
    assert "metadata" in profile
    assert profile["metadata"]["sample_values"] == []
    assert profile["metadata"]["distinct_count"] == 0
