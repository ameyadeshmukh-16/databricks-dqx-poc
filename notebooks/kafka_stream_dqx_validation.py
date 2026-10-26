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
  kafka_producer.py (local) / kafka_producer_job.py (Databricks Job)
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
     is running locally (or the kafka_producer_job is active) to observe
     real-time clean/quarantine splitting.

Job mode: query.awaitTermination() at the end keeps the Databricks Job alive
          until the stream is explicitly stopped. display() is a no-op in job
          context and does not cause errors.
"""

# -----------------------------------
# Imports
# -----------------------------------
from pyspark.sql.functions import col, from_json, current_timestamp, expr, size
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType,
)
import os
from datetime import datetime
from dotenv import load_dotenv

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
import inspect as _inspect, os as _os, sys as _sys
_repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(_inspect.currentframe().f_code.co_filename)))
if _repo_root not in _sys.path:
    _sys.path.insert(0, _repo_root)

# -----------------------------------
# Logging
# -----------------------------------
from src.logger import get_logger
logger = get_logger(__name__)

logger.info("NOTEBOOK STARTED")

# -----------------------------------
# Constants
# -----------------------------------
import uuid
from config.constants import (
    TABLE_STREAM_CLEAN,
    TABLE_STREAM_QUARANTINE,
    TABLE_BATCH_METRICS,
    TABLE_RULES,
    TABLE_STREAM_READY,
    DATABASE,
    STREAMING_TRIGGER_INTERVAL,
    STREAMING_RULE_CONFIG_NAME,
    CHECKPOINT_KAFKA,
)

# -----------------------------------
# Spark Session
# -----------------------------------
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# Enable Delta schema evolution
spark.conf.set(
    "spark.databricks.delta.schema.autoMerge.enabled",
    "true"
)

# -----------------------------------
# Storage Config
# -----------------------------------
# Grant Spark access to ADLS Gen2 for checkpoint state and Delta table writes
spark.conf.set(
    "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"],
)

# -----------------------------------
# Kafka Config
# -----------------------------------
from rtm_kafka.config.kafka_config import KAFKA_SPARK_OPTIONS, TOPIC

logger.info("Kafka topic : %s", TOPIC)
logger.info("Bootstrap   : %s", KAFKA_SPARK_OPTIONS["kafka.bootstrap.servers"])

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
        location=TABLE_RULES,
        run_config_name=STREAMING_RULE_CONFIG_NAME,
    )
)

logger.info("Loaded %d streaming rules from Delta table", len(checks))

# -----------------------------------
# Custom Check Functions
# -----------------------------------
# Required because our rules use the "sql_expression" function type.
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# -----------------------------------
# Schema
# -----------------------------------
# KEEP THIS IN SYNC WITH event_generator.py — if the schema does not match
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

logger.info("Stream ready")

# -----------------------------------
# Create Tables
# -----------------------------------
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_STREAM_CLEAN} (
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
            user_metadata : MAP<STRING, STRING>,
            rule_fingerprint : STRING,
            rule_set_fingerprint : STRING,
            skipped       : BOOLEAN
        >>
    ) USING DELTA
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_STREAM_QUARANTINE} (
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
            user_metadata : MAP<STRING, STRING>,
            rule_fingerprint : STRING,
            rule_set_fingerprint : STRING,
            skipped       : BOOLEAN
        >>,
        _warnings ARRAY<STRUCT<
            name          : STRING,
            message       : STRING,
            columns       : ARRAY<STRING>,
            filter        : STRING,
            function      : STRING,
            run_time      : TIMESTAMP,
            run_id        : STRING,
            user_metadata : MAP<STRING, STRING>,
            rule_fingerprint : STRING,
            rule_set_fingerprint : STRING,
            skipped       : BOOLEAN
        >>
    ) USING DELTA
""")

# Batch-level metrics table — written from foreachBatch so each processed micro-batch
# records its clean/quarantine split. Queried via display() below for live notebook monitoring.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_BATCH_METRICS} (
        batch_id          BIGINT,
        clean_count       BIGINT,
        quarantine_count  BIGINT,
        total_count       BIGINT,
        processed_at      TIMESTAMP
    ) USING DELTA
""")

logger.info("Tables ready")

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
      6. Write one metrics row to stream_batch_metrics for live notebook monitoring
    """
    logger.info("Batch %d started", batch_id)

    if batch_df.rdd.isEmpty():
        logger.info("Batch %d: empty, skipping", batch_id)
        return

    # Apply all DQX checks; adds _errors and _warnings columns
    dq_df = dqx.apply_checks_by_metadata(
        batch_df,
        checks,
        custom_check_functions=custom_check_functions,
    )

    # Normalise NULL arrays to empty arrays to keep Delta schema consistent
    dq_df = dq_df.withColumn(
        "_warnings",
        expr("CASE WHEN _warnings IS NULL THEN array() ELSE _warnings END"),
    )
    dq_df = dq_df.withColumn(
        "_errors",
        expr("CASE WHEN _errors IS NULL THEN array() ELSE _errors END"),
    )

    # Split: records with no errors go to clean; any error sends to quarantine
    clean      = dq_df.filter(size(col("_errors")) == 0).drop("_errors")
    quarantine = dq_df.filter(size(col("_errors")) > 0)

    clean_count      = clean.count()
    quarantine_count = quarantine.count()

    #clean.write.mode("append").saveAsTable(TABLE_STREAM_CLEAN)
    clean.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_STREAM_CLEAN)

    #quarantine.write.mode("append").saveAsTable(TABLE_STREAM_QUARANTINE)
    quarantine.write \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(TABLE_STREAM_QUARANTINE)

    logger.info(
        "Batch %d: %d clean | %d quarantined",
        batch_id, clean_count, quarantine_count,
    )

    # Write one aggregate metrics row per batch (clean/quarantine totals)
    (
        batch_df.sparkSession
            .createDataFrame(
                [(batch_id, clean_count, quarantine_count, clean_count + quarantine_count)],
                ["batch_id", "clean_count", "quarantine_count", "total_count"],
            )
            .withColumn("processed_at", current_timestamp())
            .write.mode("append")
            .saveAsTable(TABLE_BATCH_METRICS)
    )

    # Write per-rule violation breakdown to dq_rule_metrics
    from ai_suggester.rule_metrics import compute_rule_metrics, write_rule_metrics

    _run_id = f"stream_{batch_id}_{int(datetime.now().timestamp())}"
    _metrics = compute_rule_metrics(
        quarantine, clean_count + quarantine_count, _run_id, "streaming", STREAMING_RULE_CONFIG_NAME,
    )
    write_rule_metrics(_metrics, batch_df.sparkSession)

# -----------------------------------
# Start Stream
# -----------------------------------
query = (
    raw_stream.writeStream
        .foreachBatch(process)
        .option("checkpointLocation", CHECKPOINT_KAFKA)
        .trigger(processingTime=STREAMING_TRIGGER_INTERVAL)
        .start()
)

logger.info("Kafka stream started. Run kafka_producer.py or the Kafka Producer Job to send events.")

# -----------------------------------
# Readiness Signal
# -----------------------------------
# Written once the stream is actively listening, so the producer job (which
# starts concurrently in rtm_kafka_streaming — Databricks depends_on can't
# express "wait for a task that never terminates") can poll for this instead
# of sending events into a consumer that isn't subscribed yet.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_STREAM_READY} (
        started_at TIMESTAMP,
        run_id     STRING
    ) USING DELTA
""")
spark.createDataFrame(
    [(datetime.now(), str(uuid.uuid4()))],
    ["started_at", "run_id"],
).write.format("delta").mode("append").saveAsTable(TABLE_STREAM_READY)
logger.info("Readiness signal written to %s", TABLE_STREAM_READY)

# -----------------------------------
# Live Batch Monitor
# -----------------------------------
# display() on a streaming Delta source auto-refreshes in the Databricks notebook
# output pane every time a new batch writes to stream_batch_metrics.
# In interactive mode this shows a live table; in job mode it is a no-op.
display(
    spark.readStream
        .format("delta")
        .table(TABLE_BATCH_METRICS)
)

# -----------------------------------
# Keep Job Alive
# -----------------------------------
# awaitTermination() blocks until the stream is stopped (manually or on error).
# This is required when running as a Databricks Job — without it the job exits
# immediately after starting the stream, terminating the driver.
query.awaitTermination()
