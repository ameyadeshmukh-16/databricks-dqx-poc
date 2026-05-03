"""
Kafka Streaming DQX Validation (Databricks — Kafka Protocol)

Replaces the Event Hubs SDK-based streaming pipeline with a Kafka-native
approach using Azure Event Hubs Kafka protocol endpoint (port 9093, SASL_SSL).

Transport layer change summary:
  BEFORE : spark.readStream.format("eventhubs").options(**eh_conf)
  AFTER  : spark.readStream.format("kafka").options(**KAFKA_SPARK_OPTIONS)

Everything downstream — DQX rules, foreachBatch logic, clean/quarantine Delta
tables — is unchanged. Rules are loaded from the governed Delta table
(retail_dqx_poc.dq_rules_registry) rather than from YAML, consistent with the
latest pattern established in streaming_dqx_validation_adls.py.

Architecture:
  kafka_producer.py (local)
       ↓  SASL_SSL / port 9093
  Azure Event Hubs (Kafka endpoint)
       ↓  .format("kafka")
  Spark Structured Streaming  [this file]
       ↓  foreachBatch
  DQX Engine (Delta-driven rules)
       ↓
  retail_dqx_poc.stream_events_clean
  retail_dqx_poc.stream_events_quarantine

Run: execute this notebook on a Databricks cluster while kafka_producer.py
     is running locally to observe real-time clean/quarantine splitting.
"""

# -----------------------------------
# Imports
# -----------------------------------
from pyspark.sql.functions import col, from_json, current_timestamp, expr, size
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType
)
import os
from dotenv import load_dotenv

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

print("NOTEBOOK STARTED")

# -----------------------------------
# Spark Session
# -----------------------------------
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# -----------------------------------
# Storage Config
# -----------------------------------
# Grant Spark access to ADLS Gen2 for checkpoint state and Delta table writes
spark.conf.set(
    "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"]
)

# -----------------------------------
# Kafka Config
# -----------------------------------
# Import centralised Kafka options from kafka/config/kafka_config.py.
# This includes bootstrap servers, topic subscription, and SASL_SSL credentials
# for Azure Event Hubs — no connection details are hardcoded here.
from rtm_kafka.config.kafka_config import KAFKA_SPARK_OPTIONS, TOPIC

print(f"📡 Kafka topic : {TOPIC}")
print(f"📡 Bootstrap   : {KAFKA_SPARK_OPTIONS['kafka.bootstrap.servers']}")

# -----------------------------------
# DQX Rules (Delta-Driven)
# -----------------------------------
# Rules are loaded from the central Delta table rather than from YAML.
# This decouples rule management from execution and enables governed,
# reusable rule definitions shared across batch and streaming pipelines.
from unittest.mock import MagicMock
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig

dqx = DQEngine(MagicMock(spec=WorkspaceClient))

checks = dqx.load_checks(
    config=TableChecksStorageConfig(
        location="retail_dqx_poc.dq_rules_registry",   # Central Delta rule registry
        run_config_name="streaming_event_rules"         # Only streaming rules are loaded
    )
)

print(f"✅ Loaded {len(checks)} streaming rules from Delta table")

# -----------------------------------
# Custom Check Functions
# -----------------------------------
# Required because our rules use the "sql_expression" function type.
# DQX needs this mapping to know how to evaluate SQL-based conditions.
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# -----------------------------------
# Schema
# -----------------------------------
# KEEP THIS IN SYNC WITH kafka_producer.py — if the schema does not match
# the incoming JSON payload the stream will silently produce null fields.
event_schema = StructType([
    StructField("event_id",        StringType(),  True),
    StructField("store_id",        StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("division",        StringType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("unit_price",      DoubleType(),  True),
    StructField("coupon_applied",  BooleanType(), True),
    StructField("event_timestamp", StringType(),  True),
])

# -----------------------------------
# Read Stream
# -----------------------------------
# Kafka messages arrive with a binary 'value' column.
# We cast it to string, parse the JSON payload against our schema,
# then flatten the nested struct with select("data.*").
raw_stream = (
    spark.readStream
        .format("kafka")
        .options(**KAFKA_SPARK_OPTIONS)
        .load()
        .select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        )
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())
)

print("---Stream ready---")

# -----------------------------------
# Create Tables
# -----------------------------------
# Using full STRUCT schema for _errors and _warnings to preserve rich DQX
# metadata (rule name, message, affected columns, run_id, timestamps).
# This matches the schema used in streaming_dqx_validation_adls.py.
spark.sql("CREATE DATABASE IF NOT EXISTS retail_dqx_poc")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_dqx_poc.stream_events_clean (
        event_id        STRING,
        store_id        STRING,
        product_id      STRING,
        division        STRING,
        quantity        INT,
        unit_price      DOUBLE,
        coupon_applied  BOOLEAN,
        event_timestamp STRING,
        ingested_at     TIMESTAMP,
        _warnings ARRAY<STRUCT<
            name          : STRING,
            message       : STRING,
            columns       : ARRAY<STRING>,
            filter        : STRING,
            function      : STRING,
            run_time      : TIMESTAMP,
            run_id        : STRING,
            user_metadata : MAP<STRING, STRING>
        >>
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_dqx_poc.stream_events_quarantine (
        event_id        STRING,
        store_id        STRING,
        product_id      STRING,
        division        STRING,
        quantity        INT,
        unit_price      DOUBLE,
        coupon_applied  BOOLEAN,
        event_timestamp STRING,
        ingested_at     TIMESTAMP,
        _errors ARRAY<STRUCT<
            name          : STRING,
            message       : STRING,
            columns       : ARRAY<STRING>,
            filter        : STRING,
            function      : STRING,
            run_time      : TIMESTAMP,
            run_id        : STRING,
            user_metadata : MAP<STRING, STRING>
        >>,
        _warnings ARRAY<STRUCT<
            name          : STRING,
            message       : STRING,
            columns       : ARRAY<STRING>,
            filter        : STRING,
            function      : STRING,
            run_time      : TIMESTAMP,
            run_id        : STRING,
            user_metadata : MAP<STRING, STRING>
        >>
    ) USING DELTA
""")

print("---Tables ready---")

# -----------------------------------
# foreachBatch — Micro-Batch Processing
# -----------------------------------
def process(batch_df, batch_id):
    """
    Process a single Spark Structured Streaming micro-batch.

    Steps:
      1. Skip empty batches (no-op)
      2. Apply DQX rules loaded from Delta table
      3. Normalise _errors / _warnings to empty arrays (never NULL) for consistent schema
      4. Split on _errors: records with errors → quarantine, rest → clean
      5. Append both splits to their respective Delta tables
    """
    print(f"---Batch {batch_id}---")

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: empty, skipping")
        return

    # Apply all DQX checks; adds _errors and _warnings columns
    dq_df = dqx.apply_checks_by_metadata(
        batch_df,
        checks,
        custom_check_functions=custom_check_functions   # Required for sql_expression rules
    )

    # Normalise NULL arrays to empty arrays to keep Delta schema consistent
    dq_df = dq_df.withColumn(
        "_warnings",
        expr("CASE WHEN _warnings IS NULL THEN array() ELSE _warnings END")
    )
    dq_df = dq_df.withColumn(
        "_errors",
        expr("CASE WHEN _errors IS NULL THEN array() ELSE _errors END")
    )

    # Split: records with no errors go to clean; any error sends to quarantine
    clean      = dq_df.filter(size(col("_errors")) == 0).drop("_errors")
    quarantine = dq_df.filter(size(col("_errors")) > 0)

    clean_count      = clean.count()
    quarantine_count = quarantine.count()

    clean.write.mode("append").saveAsTable("retail_dqx_poc.stream_events_clean")
    quarantine.write.mode("append").saveAsTable("retail_dqx_poc.stream_events_quarantine")

    print(f"✅ Batch {batch_id}: {clean_count} clean | {quarantine_count} quarantined")

# -----------------------------------
# Start Stream
# -----------------------------------
# Dedicated checkpoint path — kept separate from the Event Hubs checkpoint
# so both pipelines can coexist without conflict during migration.
CHECKPOINT_PATH = (
    "abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net"
    "/checkpoints/kafka_streaming_dqx"
)

query = (
    raw_stream.writeStream
        .foreachBatch(process)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start()
)

print("✅ Kafka stream started. Run kafka/producer/kafka_producer.py to send events.")
