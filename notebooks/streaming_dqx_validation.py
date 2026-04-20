# Title: Streaming DQX Validation (Databricks Connect Compatible)

from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType
)
from dotenv import load_dotenv
import os, yaml

# Load env
load_dotenv()

# Databricks Connect Spark session
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# -------------------------------
# Storage config
# -------------------------------
spark.conf.set(
    "fs.azure.account.key.databricksdqxpocstorage.dfs.core.windows.net",
    os.environ["AZURE_STORAGE_KEY"]
)

# -------------------------------
# Load rules (LOCAL FILE)
# -------------------------------
with open("configs/rules_streaming.yml", "r") as f:
    rule_config = yaml.safe_load(f)

# -------------------------------
# Rule builder
# -------------------------------
def build_dqx_rules(rule_config: dict) -> list:
    rules = []
    for r in rule_config["rules"]:
        if r["type"] == "is_not_null":
            condition = f"{r['column']} IS NOT NULL"
        elif r["type"] == "is_in_list":
            values = ",".join([f"'{v}'" for v in r["allowed_values"]])
            condition = f"{r['column']} IN ({values})"
        elif r["type"] == "min_value":
            condition = f"{r['column']} >= {r['min']}"
        elif r["type"] == "matches_regex":
            condition = f"{r['column']} RLIKE '{r['regex']}'"
        elif r["type"] == "custom_sql":
            condition = r["sql_expression"]
        else:
            continue

        rules.append({
            "name": r["name"],
            "check": {
                "function": "sql_expression",
                "arguments": {"expression": condition}
            },
            "criticality": (
                "warn" if r.get("criticality", "error") == "warning"
                else r.get("criticality", "error")
            ),
            "user_metadata": {"message": r["message"]}
        })
    return rules

# -------------------------------
# Event Hub config
# -------------------------------
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONN_STR"]
EVENTHUB_NAME = "retail-pos-stream"

eh_conf = {
    "eventhubs.connectionString": EVENTHUB_CONN_STR,
    "eventhubs.eventHubName": EVENTHUB_NAME,
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}'
}

# -------------------------------
# Schema
# -------------------------------
event_schema = StructType([
    StructField("event_id",         StringType(),  True),
    StructField("store_id",         StringType(),  True),
    StructField("product_id",       StringType(),  True),
    StructField("division",         StringType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("coupon_applied",   BooleanType(), True),
    StructField("event_timestamp",  StringType(),  True),
])

# -------------------------------
# Read Stream
# -------------------------------
raw_stream = (
    spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
        .select(from_json(col("body").cast("string"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("ingested_at", current_timestamp())
)

# -------------------------------
# Create tables
# -------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS retail_dqx_poc")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_dqx_poc.stream_events_clean (
        event_id STRING, store_id STRING, product_id STRING,
        division STRING, quantity INT, unit_price DOUBLE,
        coupon_applied BOOLEAN, event_timestamp STRING,
        ingested_at TIMESTAMP, _warnings ARRAY<STRING>
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_dqx_poc.stream_events_quarantine (
        event_id STRING, store_id STRING, product_id STRING,
        division STRING, quantity INT, unit_price DOUBLE,
        coupon_applied BOOLEAN, event_timestamp STRING,
        ingested_at TIMESTAMP, _errors ARRAY<STRING>, _warnings ARRAY<STRING>
    ) USING DELTA
""")

# -------------------------------
# foreachBatch (FIXED)
# -------------------------------
def process_micro_batch(batch_df, batch_id):

    if batch_df.count() == 0:
        print(f"Batch {batch_id}: empty")
        return

    from pyspark.sql.functions import col
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine

    # Get Spark session
    spark = batch_df.sparkSession

    # Recreate DQX
    ws = WorkspaceClient()
    dqx_local = DQEngine(ws)

    # Rebuild rules
    checks_local = build_dqx_rules(rule_config)

    # Apply DQX
    dq_df = dqx_local.apply_checks_by_metadata(batch_df, checks_local)

    clean_df = dq_df.filter(col("_errors").isNull())
    quarantine_df = dq_df.filter(col("_errors").isNotNull())

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    clean_df.write.format("delta").mode("append").saveAsTable("retail_dqx_poc.stream_events_clean")

    quarantine_df.write.format("delta").mode("append").saveAsTable("retail_dqx_poc.stream_events_quarantine")

    print(f"Batch {batch_id}: {clean_count} clean | {quarantine_count} bad")

# -------------------------------
# Start Stream
# -------------------------------
query = (
    raw_stream.writeStream
        .foreachBatch(process_micro_batch)
        .option(
            "checkpointLocation",
            "abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net/checkpoints/streaming_dqx"
        )
        .trigger(processingTime="30 seconds")
        .start()
)

print("✅ Stream started. Run simulator now.")