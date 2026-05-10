"""
Central constants — single source of truth for all hardcoded values.

Import from here instead of repeating strings across notebooks and modules.
Changing a value here propagates everywhere automatically.
"""

# -----------------------------------
# Database
# -----------------------------------
CATALOG = "dqx_poc_cluster_7405612409243268"
DATABASE = "retail_dqx_poc"

# -----------------------------------
# Table names (fully qualified)
# -----------------------------------
TABLE_STREAM_CLEAN       = f"{CATALOG}.{DATABASE}.stream_events_clean"
TABLE_STREAM_QUARANTINE  = f"{CATALOG}.{DATABASE}.stream_events_quarantine"
TABLE_BATCH_METRICS      = f"{CATALOG}.{DATABASE}.stream_batch_metrics"
TABLE_RULES              = f"{CATALOG}.{DATABASE}.dq_rules_registry"
TABLE_STAGING            = f"{CATALOG}.{DATABASE}.sales_staging"
TABLE_SALES_CLEAN        = f"{CATALOG}.{DATABASE}.sales_clean"
TABLE_SALES_QUARANTINE   = f"{CATALOG}.{DATABASE}.sales_quarantine"

# -----------------------------------
# Streaming
# -----------------------------------
STREAMING_TRIGGER_INTERVAL   = "30 seconds"
STREAMING_RULE_CONFIG_NAME   = "streaming_event_rules"
BATCH_RULE_CONFIG_NAME       = "sales_batch_rules"

# -----------------------------------
# ADLS Gen2
# -----------------------------------
STORAGE_ACCOUNT   = "databricksdqxpocstorage"
CONTAINER         = "databricks-dqx"
ADLS_BASE         = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CHECKPOINT_KAFKA  = f"{ADLS_BASE}/checkpoints/kafka_streaming_dqx"
CHECKPOINT_EH     = f"{ADLS_BASE}/checkpoints/streaming_dqx"
STREAM_INPUT_PATH = f"{ADLS_BASE}/stream_input"

# -----------------------------------
# Producer simulation
# -----------------------------------
PRODUCER_BATCH_SIZE     = 10
PRODUCER_INTERVAL_SECS  = 5

# Bad-data injection rates — keep in sync with DQX rules in dq_rules_registry
BAD_DATA_RATE_EVENT_ID = 0.02   # 2%  → triggers event_id_not_null
BAD_DATA_RATE_QUANTITY = 0.06   # 6%  → triggers quantity_positive
BAD_DATA_RATE_PRICE    = 0.04   # 4%  → triggers price_positive

VALID_DIVISIONS   = ["011", "012", "013"]
INVALID_DIVISIONS = ["540", None, "999"]
ALL_DIVISIONS     = VALID_DIVISIONS + INVALID_DIVISIONS
