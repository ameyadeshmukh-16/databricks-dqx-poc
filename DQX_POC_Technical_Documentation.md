# DQX POC — Technical Deep Dive Documentation

> **Audience:** Technical leads, engineers, architects
> **Branch:** `main` (latest)
> **Last updated:** August 2026

---

## Table of Contents

1. [Repository Structure](#1-repository-structure)
2. [Architecture Overview](#2-architecture-overview)
3. [Configuration Layer](#3-configuration-layer)
4. [Infrastructure](#4-infrastructure)
5. [Phase 1 — Batch Pipeline](#5-phase-1--batch-pipeline)
6. [Phase 2 — Streaming Pipeline](#6-phase-2--streaming-pipeline)
7. [Phase 3 — AI Rule Suggester](#7-phase-3--ai-rule-suggester)
8. [Phase 4 — Quarantine Analyzer](#8-phase-4--quarantine-analyzer)
9. [Phase 5 — Rule Effectiveness Dashboard](#9-phase-5--rule-effectiveness-dashboard)
10. [Stage A — Data Quality Alerting](#10-stage-a--data-quality-alerting)
11. [Delta Table Schemas](#11-delta-table-schemas)
12. [Cross-Module Dependency Graph](#12-cross-module-dependency-graph)
13. [Rule Lifecycle State Machine](#13-rule-lifecycle-state-machine)
14. [Test Coverage](#14-test-coverage)
15. [Environment Variables Reference](#15-environment-variables-reference)
16. [Databricks Jobs Reference](#16-databricks-jobs-reference)

---

## 1. Repository Structure

```
databricks-dqx-poc/
│
├── config/
│   ├── __init__.py
│   ├── constants.py          ← All hardcoded values (table names, thresholds, rates)
│   └── settings.py           ← Settings dataclass loaded from env vars
│
├── src/
│   └── logger.py             ← Structured logging (replaces print() everywhere)
│
├── rtm_kafka/
│   ├── config/
│   │   └── kafka_config.py   ← Kafka connection params (producer + Spark consumer)
│   └── producer/
│       ├── event_generator.py ← generate_event() — single source of truth for POS events
│       └── kafka_producer.py  ← Local CLI producer (confluent-kafka)
│
├── ai_suggester/
│   ├── __init__.py
│   ├── profiler.py            ← profile_dataframe() — per-column statistics
│   ├── rule_generator.py      ← suggest_statistical_rules() — no LLM needed
│   ├── llm_backend.py         ← Pluggable LLM: Claude / Azure OpenAI / OpenRouter
│   ├── llm_rule_suggester.py  ← suggest_llm_rules() — prompt builder + response parser
│   ├── rule_writer.py         ← write/promote/reject suggestions in Delta
│   ├── quarantine_analyzer.py ← extract_violation_patterns() + suggest_rules_from_quarantine()
│   ├── rule_metrics.py        ← compute_rule_metrics() + write_rule_metrics()
│   └── alerting.py            ← detect_spikes() + Slack notification + dq_alert_log audit trail
│
├── notebooks/
│   ├── setup_delta_tables.py          ← Batch: load CSV → sales_staging
│   ├── batch_dqx_validation.py        ← Batch: validate + split clean/quarantine
│   ├── kafka_producer_job.py          ← Databricks Job: event producer
│   ├── kafka_stream_dqx_validation.py ← Streaming: consume + validate + split
│   ├── ai_rule_suggester.py           ← AI Rule Suggester notebook (5 steps)
│   ├── quarantine_analyzer.py         ← Quarantine Analyzer notebook (4 steps)
│   ├── rule_effectiveness_report.py   ← Effectiveness Dashboard notebook (4 panels)
│   ├── dq_alert_check.py              ← Stage A: spike detection + Slack alert notebook
│   └── [legacy, superseded by the Kafka/Delta-registry pipeline above — kept for
│        reference, not wired into any job]: streaming_dqx_validation.py,
│        streaming_dqx_validation_adls.py, stream_simulator*.py,
│        kafka_consumer_simple.py, kafka_read_stream.py, sample_data_generation.py,
│        testing.py
│
├── tests/
│   ├── __init__.py
│   ├── test_profiler.py            ← 10 tests (PySpark, local session)
│   ├── test_rule_generator.py      ← 16 tests (pure Python)
│   ├── test_quarantine_analyzer.py ← 14 tests (pure Python)
│   ├── test_rule_metrics.py        ← 9 tests (pure Python)
│   ├── test_alerting.py            ← 11 tests (pure Python)
│   ├── test_config.py              ← 5 tests (pure Python)
│   └── test_event_generator.py     ← 9 tests (pure Python)
│
├── configs/
│   ├── rules_batch.yml            ← Legacy YAML rule examples — NOT loaded by any
│   └── rules_streaming.yml          current pipeline; live rules come from the
│                                     dq_rules_registry Delta table (see §7.5)
│
├── databricks.yml                 ← Databricks Asset Bundle job definitions
├── requirements.txt               ← Pinned Python dependencies
├── .env.example                   ← Environment variable template
└── DQX_POC_Documentation.md       ← Management-facing documentation
```

---

## 2. Architecture Overview

### 2.1 Full System Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              INFRASTRUCTURE                                     │
│                                                                                 │
│   Azure ADLS Gen2                        Azure Event Hubs (Kafka endpoint)      │
│   databricksdqxpocstorage                evhns-dqx-poc.servicebus.windows.net   │
│   Container: databricks-dqx             Topic: retail-pos-stream                │
│   ├── sales_transactions.csv            Port: 9093 (SASL_SSL / PLAIN)           │
│   └── checkpoints/                                                              │
│                                         Azure Key Vault: kv-dqx-poc             │
│                                         Secret Scope: kv-dqx-scope             │
└─────────────────────────────────────────────────────────────────────────────────┘
                    │                                      │
                    ▼                                      ▼
┌───────────────────────────┐           ┌─────────────────────────────────────────┐
│    BATCH PIPELINE         │           │         STREAMING PIPELINE              │
│                           │           │                                         │
│ setup_delta_tables.py     │           │ kafka_producer_job.py (Databricks Job)  │
│   · Read CSV from ADLS    │           │   OR                                    │
│   · Write → sales_staging │           │ kafka_producer.py (local CLI)           │
│                           │           │   · confluent-kafka Producer            │
│ batch_dqx_validation.py   │           │   · generate_event() → JSON → Kafka     │
│   · DQEngine.load_checks()│           │                                         │
│   · apply_checks_by_      │           │ kafka_stream_dqx_validation.py          │
│     metadata()            │           │   · readStream.format("kafka")          │
│   · Split on _errors      │           │   · DQEngine.load_checks() at startup   │
│   · → sales_clean         │           │   · foreachBatch(process)               │
│   · → sales_quarantine    │           │   · → stream_events_clean               │
│   · → dq_rule_metrics     │           │   · → stream_events_quarantine          │
└────────────┬──────────────┘           │   · → stream_batch_metrics              │
             │                          │   · → dq_rule_metrics                   │
             │                          └──────────────────┬──────────────────────┘
             │                                             │
             │      Both pipelines read rules from:        │
             │      ╔════════════════════════════╗         │
             └─────►║   dq_rules_registry        ║◄────────┘
                    ║   (Unity Catalog Delta)     ║
                    ╚══════════════╤═════════════╝
                                   │ promote_rule()
                    ╔══════════════▼═════════════╗
                    ║   dq_rules_staging         ║
                    ║   status: pending →        ║
                    ║   approved | rejected      ║
                    ╚══════════════╤═════════════╝
                                   │ AI writes here
               ┌───────────────────┴────────────────────┐
               │                                        │
    ┌──────────▼──────────┐               ┌─────────────▼───────────┐
    │  AI RULE SUGGESTER  │               │   QUARANTINE ANALYZER   │
    │                     │               │                         │
    │  profile_dataframe()│               │  extract_violation_     │
    │  suggest_           │               │    patterns()           │
    │    statistical_     │               │  suggest_rules_from_    │
    │    rules()          │               │    quarantine()         │
    │  suggest_llm_rules()│               │                         │
    │  LLM Backend:       │               │  LLM Backend:           │
    │  Claude /           │               │  Claude /               │
    │  Azure OpenAI /     │               │  Azure OpenAI /         │
    │  OpenRouter         │               │  OpenRouter             │
    └─────────────────────┘               └─────────────────────────┘

                    ╔════════════════════════════╗
                    ║   dq_rule_metrics          ║
                    ║   (per-rule violation       ║
                    ║    counts per run)          ║
                    ╚══════════════╤═════════════╝
                                   │
                    ┌──────────────▼──────────────┐
                    │   LAKEVIEW DASHBOARD        │
                    │   rule_effectiveness_       │
                    │   report.py                 │
                    │   · Effectiveness Summary   │
                    │   · Violation Trend         │
                    │   · Top Offenders           │
                    │   · Zero-fire Candidates    │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   ALERTING (Stage A)        │
                    │   ai_suggester/alerting.py  │
                    │   · detect_spikes()         │
                    │   · Slack webhook notify    │
                    │   · → dq_alert_log          │
                    │   (also: native SQL Alert   │
                    │    on dq_rule_metrics)       │
                    └─────────────────────────────┘
```

### 2.2 Data Flow Summary

```
Raw Data
  │
  ├──[Batch]──► sales_staging ──► DQX Validate ──► sales_clean
  │                                             └──► sales_quarantine ──► dq_rule_metrics
  │
  └──[Stream]─► Kafka Topic  ──► DQX Validate ──► stream_events_clean
                                              └──► stream_events_quarantine ──► dq_rule_metrics
                                                          │
                                                    Quarantine Analyzer
                                                          │
                                              ┌───────────┴──────────┐
                                              ▼                      ▼
                                     dq_rules_staging     dq_rules_staging
                                    (source=quarantine)  (source=llm/statistical)
                                              │                      │
                                              └──────────┬───────────┘
                                                         ▼
                                              Human Review (approve/reject)
                                                         │
                                                         ▼
                                              dq_rules_registry (live)
```

---

## 3. Configuration Layer

### 3.1 `config/constants.py`

Single source of truth for all string constants. Every notebook and module imports from here.

**Key constants:**

```python
# Unity Catalog coordinates
CATALOG  = "dqx_poc_cluster_7405619883480512"
DATABASE = "retail_dqx_poc"

# Table names (fully qualified: CATALOG.DATABASE.table)
TABLE_STREAM_CLEAN       = f"{CATALOG}.{DATABASE}.stream_events_clean"
TABLE_STREAM_QUARANTINE  = f"{CATALOG}.{DATABASE}.stream_events_quarantine"
TABLE_BATCH_METRICS      = f"{CATALOG}.{DATABASE}.stream_batch_metrics"
TABLE_RULES              = f"{CATALOG}.{DATABASE}.dq_rules_registry"
TABLE_STAGING            = f"{CATALOG}.{DATABASE}.sales_staging"
TABLE_SALES_CLEAN        = f"{CATALOG}.{DATABASE}.sales_clean"
TABLE_SALES_QUARANTINE   = f"{CATALOG}.{DATABASE}.sales_quarantine"
TABLE_RULES_STAGING      = f"{CATALOG}.{DATABASE}.dq_rules_staging"
TABLE_RULE_METRICS       = f"{CATALOG}.{DATABASE}.dq_rule_metrics"

# Rule group identifiers (must match dq_rules_registry.run_config_name)
STREAMING_RULE_CONFIG_NAME = "streaming_event_rules"
BATCH_RULE_CONFIG_NAME     = "sales_batch_rules"

# ADLS paths
ADLS_BASE        = "abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net"
CHECKPOINT_KAFKA = f"{ADLS_BASE}/checkpoints/kafka_streaming_dqx"

# Bad-data injection rates (producer simulation)
BAD_DATA_RATE_EVENT_ID = 0.02   # 2%
BAD_DATA_RATE_QUANTITY = 0.06   # 6%
BAD_DATA_RATE_PRICE    = 0.04   # 4%
VALID_DIVISIONS        = ["011", "012", "013"]
INVALID_DIVISIONS      = ["540", None, "999"]

# AI Rule Suggester thresholds
PROFILER_SAMPLE_SIZE    = 10_000
STAT_NULL_THRESHOLD     = 0.01   # null_pct < 1%  → suggest is_not_null
STAT_DISTINCT_THRESHOLD = 50     # ≤ 50 distinct  → consider is_in_list
STAT_COVERAGE_THRESHOLD = 0.95   # ≥ 95% non-null → use as allowlist
```

**Design note:** Changing `CATALOG` at line 11 automatically propagates to all 9 `TABLE_*` constants. No other file needs updating.

---

### 3.2 `config/settings.py`

Dataclass loaded from environment variables. Centralises all `os.environ` reads and enforces required-variable validation at startup.

**Class:** `Settings`

**Required fields** (raise `EnvironmentError` if absent):

| Field | Env Var | Description |
|---|---|---|
| `azure_storage_key` | `AZURE_STORAGE_KEY` | ADLS Gen2 account key |
| `eventhub_conn_str` | `EVENTHUB_CONN_STR` | Event Hubs namespace connection string (no EntityPath) |
| `kafka_bootstrap_servers` | `KAFKA_BOOTSTRAP_SERVERS` | `evhns-dqx-poc.servicebus.windows.net:9093` |
| `kafka_topic` | `KAFKA_TOPIC` | `retail-pos-stream` |
| `kafka_consumer_group` | `KAFKA_CONSUMER_GROUP` | `databricks-dqx-kafka-consumer` |

**Optional fields** (default to `""`):

| Field | Env Var | Used By |
|---|---|---|
| `anthropic_api_key` | `ANTHROPIC_API_KEY` | ClaudeBackend |
| `claude_model` | `CLAUDE_MODEL` | ClaudeBackend (default: `claude-sonnet-4-6`) |
| `azure_openai_api_key` | `AZURE_OPENAI_API_KEY` | AzureOpenAIBackend |
| `azure_openai_endpoint` | `AZURE_OPENAI_ENDPOINT` | AzureOpenAIBackend |
| `azure_openai_deployment` | `AZURE_OPENAI_DEPLOYMENT` | AzureOpenAIBackend |
| `openrouter_api_key` | `OPENROUTER_API_KEY` | OpenRouterBackend |
| `openrouter_model` | `OPENROUTER_MODEL` | OpenRouterBackend (default: `openrouter/auto`) |
| `openai_base_url` | `OPENAI_BASE_URL` | OpenRouterBackend (default: `https://openrouter.ai/api/v1`) |
| `llm_backend` | `LLM_BACKEND` | `get_backend()` selector (default: `"claude"`) |

**Key method:**
```python
Settings.from_env() -> Settings
# Reads all env vars, validates required fields, returns populated Settings instance.
# Raises EnvironmentError listing all missing required vars if any are absent.
```

---

### 3.3 `src/logger.py`

**Function:** `get_logger(name: str) -> logging.Logger`

Returns a named logger with a `StreamHandler` writing to `stdout`. Idempotent — calling `get_logger(__name__)` multiple times returns the same logger without duplicating handlers.

**Log format:**
```
2024-01-15 12:00:00 [ai_suggester.profiler] INFO — Profiling 5000 rows (full scan)
```

On Databricks, Python `logging` output appears in the driver log with timestamps and is searchable. `print()` output appears in notebook output cells only — not in job logs.

**Usage pattern (all modules):**
```python
from src.logger import get_logger
logger = get_logger(__name__)
logger.info("Message with %s formatting", variable)
logger.warning("...")
logger.error("...")
logger.debug("...")   # suppressed at INFO level unless level changed
```

---

## 4. Infrastructure

### 4.1 `databricks.yml` — Databricks Asset Bundle

Defines 3 Databricks Jobs deployable via `databricks bundle deploy --target dev`. What were originally 6 independent, manually-triggered jobs have been chained into 3 orchestrated flows (see commits `b9510e8`, `1d1c878`) — the batch stages now run as a single 6-task DAG, and the two Kafka tasks run concurrently in one job using a readiness-signal handshake instead of Databricks' `depends_on` (which requires the upstream task to reach a terminal state — impossible for a `timeout_seconds: 0` streaming task).

```
Target: dev
Workspace: https://adb-7405619883480512.12.azuredatabricks.net
Cluster ID: 0601-042329-nm42bg1s
```

**Job inventory:**

| Job Key | Display Name | Tasks | Notes |
|---|---|---|---|
| `rtm_kafka_streaming` | [RTM] Kafka Streaming Pipeline | `stream_and_validate` (`kafka_stream_dqx_validation.py`, `timeout_seconds: 0`) + `produce_events` (`kafka_producer_job.py`) | Both tasks start concurrently, no `depends_on`; producer polls a `stream_consumer_ready` signal before sending events |
| `rtm_batch_pipeline` | [RTM] Batch DQX Pipeline | `stage_data` → `validate_data` → `{suggest_rules, analyze_quarantine}` → `run_report` → `check_alerts` | 6-task DAG; `suggest_rules` and `analyze_quarantine` run in parallel off `validate_data`, `run_report` waits on both, `check_alerts` runs last |
| `dq_alert_check_streaming` | [RTM] DQ Alert Check (Streaming) | `check_alerts` (`dq_alert_check.py`, `run_config_name=streaming_event_rules`) | Cron-scheduled every 15 minutes (`0 */15 * * * ?` UTC) — the batch DAG's `check_alerts` task only covers batch rules, so streaming rules need this separate scheduled check |

See [§16 Databricks Jobs Reference](#16-databricks-jobs-reference) for full task parameters.

---

## 5. Phase 1 — Batch Pipeline

### 5.1 `notebooks/setup_delta_tables.py`

**Purpose:** Reads raw sales CSV from ADLS Gen2, writes it to the `sales_staging` Delta table.

**Execution flow:**
```
load_dotenv()
DatabricksSession.builder.getOrCreate()
spark.conf.set(AZURE_STORAGE_KEY)     ← authenticates to ADLS
dbutils.widgets: csv_path, database
spark.sql("CREATE DATABASE IF NOT EXISTS retail_dqx_poc")
spark.read.csv(csv_path) → sales_raw
sales_raw.write.delta.saveAsTable(TABLE_STAGING)
```

**Key imports:**
```python
from config.constants import DATABASE, TABLE_STAGING, ADLS_BASE, STORAGE_ACCOUNT
from src.logger import get_logger
from databricks.connect import DatabricksSession
```

**Widgets (Databricks Job parameters):**

| Widget | Default | Description |
|---|---|---|
| `csv_path` | `{ADLS_BASE}/sales_transactions.csv` | ADLS path to raw CSV |
| `database` | `retail_dqx_poc` | Target Unity Catalog database |

---

### 5.2 `notebooks/batch_dqx_validation.py`

**Purpose:** Loads DQX rules from `dq_rules_registry`, applies them to `sales_staging`, splits results into clean and quarantine tables, writes per-rule metrics.

**Execution flow:**
```
DatabricksSession.builder.getOrCreate()
ws = WorkspaceClient()
dqx = DQEngine(ws)

checks = dqx.load_checks(TableChecksStorageConfig(
    location=TABLE_RULES,
    run_config_name=BATCH_RULE_CONFIG_NAME,   # "sales_batch_rules"
))

sales_df = spark.table(TABLE_STAGING)

dq_df = dqx.apply_checks_by_metadata(
    sales_df,
    checks,
    custom_check_functions={"sql_expression": lambda df, **kw: expr(kw["expression"])},
)

good_df      = dq_df.filter(_errors IS NULL OR size(_errors) == 0)
quarantine_df = dq_df.filter(_errors IS NOT NULL AND size(_errors) > 0)

total_count, clean_count, quarantine_count = dq_df.count(), good_df.count(), quarantine_df.count()

# Rule metrics — NEW in Phase 5
run_id = str(uuid.uuid4())
metrics = compute_rule_metrics(quarantine_df, total_count, run_id, "batch", BATCH_RULE_CONFIG_NAME)
write_rule_metrics(metrics, spark)

good_df.write.delta.overwrite.saveAsTable(TABLE_SALES_CLEAN)
quarantine_df.write.delta.overwrite.saveAsTable(TABLE_SALES_QUARANTINE)
```

**Custom check function:** The `sql_expression` function type (used for cross-column rules like `transaction_date <= CURRENT_DATE`) is not a built-in DQX function. It is registered via the `custom_check_functions` dict, which maps the function name string to a lambda that returns a Spark Column expression.

**Key imports:**
```python
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.config import TableChecksStorageConfig
from ai_suggester.rule_metrics import compute_rule_metrics, write_rule_metrics
from config.constants import TABLE_RULES, TABLE_STAGING, TABLE_SALES_CLEAN,
                              TABLE_SALES_QUARANTINE, BATCH_RULE_CONFIG_NAME
```

---

## 6. Phase 2 — Streaming Pipeline

### 6.1 `rtm_kafka/config/kafka_config.py`

**Purpose:** Single source of truth for Kafka connection parameters used by both the Python producer (confluent-kafka) and the Spark consumer (JVM Kafka connector).

**Key exports:**

| Export | Type | Used by |
|---|---|---|
| `KAFKA_PRODUCER_CONFIG` | `dict` | `confluent_kafka.Producer(**KAFKA_PRODUCER_CONFIG)` |
| `KAFKA_SPARK_OPTIONS` | `dict` | `spark.readStream.format("kafka").options(**KAFKA_SPARK_OPTIONS)` |
| `TOPIC` | `str` | Both producer and consumer |
| `CONSUMER_GROUP` | `str` | Spark consumer |

**Producer config keys** (confluent-kafka dot-notation):
```python
{
    "bootstrap.servers": "evhns-dqx-poc.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     "$ConnectionString",   # literal — Azure EH requirement
    "sasl.password":     "<EVENTHUB_CONN_STR>",
}
```

**Spark consumer options** (kafka.* prefix = JVM Kafka config passthrough):
```python
{
    "kafka.bootstrap.servers": "...",
    "subscribe":               "retail-pos-stream",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  "org.apache.kafka.common.security.plain.PlainLoginModule ...",
    "kafka.group.id":          "databricks-dqx-kafka-consumer",
    "startingOffsets":         "latest",
}
```

**Why confluent-kafka and not kafka-python?**
`kafka-python`'s `KafkaProducer` is lazy — connection happens on first `.send()`, where it attempts Kafka API version auto-detection. Azure Event Hubs does not respond to this probe correctly, causing a 60-second `KafkaTimeoutError`. `confluent-kafka` (the Microsoft-recommended client) connects on instantiation with a proper handshake.

---

### 6.2 `rtm_kafka/producer/event_generator.py`

**Purpose:** Single authoritative implementation of `generate_event()`. Eliminates the 4 near-identical implementations previously scattered across `stream_simulator.py`, `stream_simulator_adls.py`, `sample_data_generation.py`, and `kafka_producer.py`.

**Function:**
```python
def generate_event(i: int) -> dict
```

**Returns a dict with fields:**

| Field | Type | Bad data injection |
|---|---|---|
| `event_id` | `str \| None` | 2% → `None` (triggers `event_id_not_null`) |
| `store_id` | `str` | None |
| `product_id` | `str` | None |
| `division` | `str \| None` | From `ALL_DIVISIONS = ["011","012","013","540",None,"999"]` |
| `quantity` | `int` | 6% → `-1` (triggers `quantity_positive`) |
| `unit_price` | `float` | 4% → `0` (triggers `price_positive`) |
| `coupon_applied` | `bool` | None |
| `event_timestamp` | `str` | ISO format |

**Bad data rates** are read from `config/constants.py` — same constants that document the DQX rules — ensuring injection rates stay aligned with rule expectations.

---

### 6.3 `rtm_kafka/producer/kafka_producer.py`

**Purpose:** Local CLI entry point for the Kafka producer. Run with `python -m rtm_kafka.producer.kafka_producer`.

**Key behaviour:**
- Instantiates `confluent_kafka.Producer(KAFKA_PRODUCER_CONFIG)`
- Loops: generate `PRODUCER_BATCH_SIZE` events → `producer.produce(TOPIC, json.dumps(event).encode())` → `producer.poll(0)` → `producer.flush()` → `sleep(PRODUCER_INTERVAL_SECS)`
- `delivery_callback(err, msg)`: logs delivery errors per message
- `producer.poll(0)`: non-blocking callback drain (queues delivery reports without waiting)
- `producer.flush()`: blocks until all in-flight messages are acknowledged before sleeping

**Databricks Job equivalent:** `notebooks/kafka_producer_job.py` — same logic, wrapped with `dbutils.widgets` for parameterisation.

---

### 6.4 `notebooks/kafka_stream_dqx_validation.py`

**Purpose:** Main Databricks streaming pipeline. Consumes from Kafka, applies DQX rules, splits into clean/quarantine, writes metrics.

**Execution flow:**

```
1. STARTUP (runs once)
   ├── Load env, configure ADLS storage key
   ├── DatabricksSession.getOrCreate()
   ├── DQEngine(WorkspaceClient())
   ├── dqx.load_checks(run_config_name="streaming_event_rules")  ← rules loaded ONCE
   ├── custom_check_functions = {"sql_expression": ...}
   ├── Define stream_events_clean schema (StructType)
   ├── Create Delta tables if not exist (TABLE_STREAM_CLEAN, TABLE_STREAM_QUARANTINE,
   │   TABLE_BATCH_METRICS)
   └── raw_stream = spark.readStream.format("kafka").options(**KAFKA_SPARK_OPTIONS)
                       .load()
                       .select(from_json(col("value").cast("string"), schema))
                       .withColumn("ingested_at", current_timestamp())

2. MICRO-BATCH (process function, called by foreachBatch every 30 seconds)
   ├── if batch_df.rdd.isEmpty(): return   ← skip empty batches
   ├── dq_df = dqx.apply_checks_by_metadata(batch_df, checks, custom_check_functions)
   ├── Normalise NULL _errors/_warnings → empty arrays (schema consistency)
   ├── clean      = dq_df.filter(size(_errors) == 0).drop("_errors")
   ├── quarantine = dq_df.filter(size(_errors) > 0)
   ├── clean_count, quarantine_count = clean.count(), quarantine.count()
   ├── clean.write.append.saveAsTable(TABLE_STREAM_CLEAN)
   ├── quarantine.write.append.saveAsTable(TABLE_STREAM_QUARANTINE)
   ├── Write aggregate row to TABLE_BATCH_METRICS (batch_id, clean/quarantine counts)
   └── compute_rule_metrics + write_rule_metrics → TABLE_RULE_METRICS  ← Phase 5

3. STREAM START
   query = raw_stream.writeStream
       .foreachBatch(process)
       .option("checkpointLocation", CHECKPOINT_KAFKA)
       .trigger(processingTime="30 seconds")
       .start()
   query.awaitTermination()   ← keeps Databricks Job alive
```

**Schema for Kafka value JSON:**
```python
StructType([
    StructField("event_id",        StringType(),  True),
    StructField("store_id",        StringType(),  True),
    StructField("product_id",      StringType(),  True),
    StructField("division",        StringType(),  True),
    StructField("quantity",        IntegerType(), True),
    StructField("unit_price",      DoubleType(),  True),
    StructField("coupon_applied",  BooleanType(), True),
    StructField("event_timestamp", StringType(),  True),
])
```

**Important constraint — rule reload:** `dqx.load_checks()` is called once at consumer startup, not inside `process()`. Rules promoted to `dq_rules_registry` after the consumer starts will not be picked up until the consumer is restarted (checkpoint must also be cleared to avoid offset conflicts).

---

## 7. Phase 3 — AI Rule Suggester

### 7.1 `ai_suggester/profiler.py`

**Function:**
```python
def profile_dataframe(df: DataFrame, sample_size: int = PROFILER_SAMPLE_SIZE) -> dict
```

**What it does:**
- If `df.count() > sample_size`: calls `df.sample(fraction=sample_size/total, seed=42)`
- Iterates over every `df.schema.fields`
- **Skips** columns starting with `_` (DQX metadata: `_errors`, `_warnings`)
- **Guards** MapType / ArrayType / StructType columns (`_COMPLEX_TYPES`) from operations that fail on nested types

**Per-column aggregation (single Spark pass):**
```python
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
```

**Returns dict per column:**
```python
{
    "dtype":           "StringType",           # Spark type class name
    "null_pct":        0.02,                   # fraction (0.0–1.0)
    "distinct_count":  6,
    "distinct_values": ["011","012","013"],     # only present when count ≤ STAT_DISTINCT_THRESHOLD
    "min":             1.0,                    # numeric only
    "max":             300.0,                  # numeric only
    "mean":            45.2,                   # numeric only
    "stddev":          18.7,                   # numeric only
    "sample_values":   ["EVT000042", ...],     # up to 5 non-null values; [] for complex types
}
```

**Guards for complex types:**
- `distinct_count = 0` (no `countDistinct` call)
- No `distinct_values` collected
- `sample_values = []` (`.rdd.flatMap` fails on MapType)

---

### 7.2 `ai_suggester/rule_generator.py`

**Function:**
```python
def suggest_statistical_rules(profile: dict, run_config_name: str) -> list[dict]
```

No Spark, no LLM — pure Python decision logic on the profile dict.

**Decision rules:**

| Condition | Generated Rule | Function | Criticality |
|---|---|---|---|
| `null_pct < STAT_NULL_THRESHOLD (0.01)` | `{col}_not_null` | `is_not_null` | `error` |
| `distinct_values` present AND `distinct_count ≤ 50` AND `not is_numeric` AND `(1 - null_pct) ≥ 0.95` | `{col}_valid_values` | `sql_expression` | `warning` |
| `is_numeric` AND `col_min > 0` | `{col}_min_value` | `min_value` with `min=0` | `error` |
| `is_numeric` AND `col_max > 0` | `{col}_max_value` | `max_value` with `max=_round_up_ceiling(col_max)` | `warning` |

**Helper:**
```python
def _round_up_ceiling(value: float) -> float
# Returns a round ceiling ~10× the observed max.
# Examples: 9 → 90, 10 → 100, 11 → 200, 99 → 1000, 299.99 → 3000
# Uses: magnitude = 10^floor(log10(value)); ceil(value/magnitude) * magnitude * 10
```

**Rule dict structure (all sources produce this format):**
```python
{
    "name":        "quantity_min_value",
    "column":      "quantity",
    "function":    "min_value",
    "constraint":  {"min": 0},
    "criticality": "error",
    "message":     "quantity must be greater than 0",
    "source":      "statistical",
    "confidence":  0.9,           # 0.0–1.0; None for LLM rules
    "reasoning":   "Observed minimum value is 1 — all sampled values are strictly positive",
}
```

---

### 7.3 `ai_suggester/llm_backend.py`

**Abstract base class:**
```python
class LLMBackend(ABC):
    @abstractmethod
    def generate(self, prompt: str, system: str = "") -> str: ...
```

**Three concrete implementations:**

#### `ClaudeBackend`
- Requires: `ANTHROPIC_API_KEY`, optional `CLAUDE_MODEL` (default: `claude-sonnet-4-6`)
- Uses: `anthropic.Anthropic(api_key=...).messages.create(model, max_tokens=4096, system, messages)`
- Returns: `message.content[0].text`

#### `AzureOpenAIBackend`
- Requires: `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_DEPLOYMENT`
- Optional: `AZURE_OPENAI_API_VERSION` (default: `2024-02-01`)
- Uses: `openai.AzureOpenAI(api_key, azure_endpoint, api_version).chat.completions.create(model=deployment, temperature=0.2, max_tokens=4096)`
- Returns: `response.choices[0].message.content`

#### `OpenRouterBackend`
- Requires: `OPENROUTER_API_KEY`
- Optional: `OPENROUTER_MODEL` (default: `openrouter/auto`), `OPENAI_BASE_URL` (default: `https://openrouter.ai/api/v1`)
- Uses: `openai.OpenAI(api_key=OPENROUTER_API_KEY, base_url=...).chat.completions.create(...)`  ← OpenAI SDK with custom base URL
- Returns: `response.choices[0].message.content`

**Factory function:**
```python
def get_backend(name: str | None = None) -> LLMBackend
# name: "claude" | "azure_openai" | "openrouter" | None
# None → reads LLM_BACKEND env var (default: "claude")
# Raises ValueError for unknown names
```

**Helper (used by response parsers):**
```python
def _backend_name(llm: LLMBackend) -> str
# ClaudeBackend → "claude"
# AzureOpenAIBackend → "azureopenai"
# OpenRouterBackend → "openrouter"
```

---

### 7.4 `ai_suggester/llm_rule_suggester.py`

**Function:**
```python
def suggest_llm_rules(
    profile: dict,
    run_config_name: str,
    context: str,
    stat_rules: list[dict] | None = None,
    backend: str | None = None,
    llm: LLMBackend | None = None,
) -> list[dict]
```

**Flow:**
```
get_backend(backend)  OR  use provided llm
_build_prompt(profile, context, stat_rules)
llm.generate(prompt, system=_SYSTEM_PROMPT)
_parse_response(raw, run_config_name, backend_name)
return validated rules
```

**`_build_prompt` structure:**
```
Dataset context: {context}

Column profiles:
  event_id (StringType): null=2.0%, distinct=4998, samples=['EVT000001', ...]
  quantity (IntegerType): null=6.0%, distinct=11, min=1, max=10, mean=5.4
  ...

Statistical rules already generated (do NOT duplicate these):
  - event_id_not_null: event_id must not be null
  - quantity_min_value: quantity must be greater than 0
  ...

Suggest business-logic data quality rules that the statistics above cannot detect.
Focus on:
  1. Cross-column integrity
  2. Domain constraints specific to retail/e-commerce
  3. Temporal or referential anomalies

Output ONLY a JSON array. No prose.
```

**System prompt** instructs the LLM to output only a JSON array with strict key requirements: `name`, `column`, `function`, `constraint`, `criticality`, `message`, `reasoning`.

**`_parse_response` logic:**
1. Strip markdown fences (```` ```json ... ``` ````)
2. `re.search(r"\[.*\]", cleaned, re.DOTALL)` — extract outermost JSON array
3. `json.loads(match.group(0))`
4. For each candidate: validate required keys, default bad `criticality` to `"warning"`, set `source="llm"`, `confidence=None`

---

### 7.5 `ai_suggester/rule_writer.py`

Three public functions.

#### `ensure_staging_table(spark) -> None`
```sql
CREATE TABLE IF NOT EXISTS dq_rules_staging (
    rule_id STRING, name STRING, column_name STRING, function STRING,
    constraint STRING, criticality STRING, message STRING,
    run_config_name STRING, source STRING, llm_backend STRING,
    confidence DOUBLE, reasoning STRING, status STRING,
    suggested_at TIMESTAMP, reviewed_at TIMESTAMP, reviewed_by STRING
) USING DELTA
```

#### `write_suggestions_to_staging(rules, run_config_name, spark) -> int`
- Generates `uuid4()` per rule as `rule_id`
- Serialises `constraint` dict as `json.dumps()`
- Sets `status = "pending"`, `suggested_at = datetime.now(utc)`
- Creates explicit `StructType` schema → `spark.createDataFrame` → `.write.append.saveAsTable`
- Returns number of rows written

#### `promote_rule(rule_id, reviewed_by, spark) -> None`
```
1. spark.sql("SELECT * FROM dq_rules_staging WHERE rule_id = '{rule_id}'")
2. Validate row["status"] == "pending" (raises ValueError otherwise)
3. spark.sql("UPDATE dq_rules_staging SET status='approved', reviewed_at=..., reviewed_by=... WHERE rule_id=...")
4. Build registry_row with DQX native nested schema:
   {
     "name":           row["name"],
     "criticality":    row["criticality"],
     "check": {
         "function":        row["function"],
         "for_each_column": [] if sql_expression else [row["column_name"]],
         "arguments":       {k: str(v) for k, v in json.loads(row["constraint"]).items()},
     },
     "filter":               None,
     "run_config_name":      row["run_config_name"],
     "user_metadata":        {"message": row["message"]},
     "created_at":           now,
     "rule_fingerprint":     None,
     "rule_set_fingerprint": None,
   }
5. spark.createDataFrame([registry_row], schema=registry_schema)
         .write.append.saveAsTable(TABLE_RULES)
```

**Key design decision — `for_each_column`:** For `sql_expression` rules, the column is embedded in the SQL expression string itself (e.g. `quantity > 0`). DQX expects `for_each_column = []` in this case. For all other function types, `for_each_column = [column_name]`.

**`rule_fingerprint` is null:** DQX computes fingerprints via its internal hasher during `DQEngine` rule loading from YAML. Rules written directly to Delta via `saveAsTable` bypass this pipeline. Null fingerprints are safe — DQX falls back to loading rules by content when fingerprints are absent.

#### `reject_rule(rule_id, reviewed_by, spark) -> None`
```sql
UPDATE dq_rules_staging
SET status = 'rejected', reviewed_at = TIMESTAMP '...', reviewed_by = '...'
WHERE rule_id = '{rule_id}'
```

---

### 7.6 `notebooks/ai_rule_suggester.py`

**Widgets:**

| Widget | Type | Default | Description |
|---|---|---|---|
| `source_table` | text | `dqx_poc_cluster_*.retail_dqx_poc.sales_staging` | Table to profile |
| `run_config_name` | text | `sales_batch_rules` | Rule group for staging |
| `context_description` | text | `Retail POS sales transactions...` | Domain context for LLM prompt |
| `llm_backend` | dropdown | `openrouter` | `openrouter \| claude \| azure_openai` |

**5-step execution:**
```
Step 1: df = spark.table(source_table)
        profile = profile_dataframe(df)                       → prints column summary

Step 2: stat_rules = suggest_statistical_rules(profile, run_config_name)
                                                               → prints rule count

Step 3: llm_rules = suggest_llm_rules(profile, run_config_name, context, stat_rules, backend)
                                                               → prints rule count (or error if LLM fails)

Step 4: all_rules = stat_rules + llm_rules
        write_suggestions_to_staging(all_rules, run_config_name, spark)
                                                               → prints written count

Step 5: display(spark.sql("SELECT * FROM dq_rules_staging WHERE status='pending'..."))
        + promotion instructions printed
```

---

## 8. Phase 4 — Quarantine Analyzer

### 8.1 `ai_suggester/quarantine_analyzer.py`

#### `extract_violation_patterns(quarantine_df, top_n=20) -> dict`

```python
# Explode _errors array → one row per violation
exploded = quarantine_df.select(
    *business_cols,                          # all columns not starting with "_"
    F.explode(F.col("_errors")).alias("_err"),
)

# Aggregate: count violations per rule
violation_summary = (
    exploded
    .groupBy("_err.name", "_err.message", "_err.function")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
    .limit(top_n)
    .collect()
)

# For each rule: sample up to 5 offending business-column rows
for row in violation_summary:
    sample_rows = exploded.filter(_err.name == row.rule_name).select(business_cols[:4]).limit(5)
```

**Returns:**
```python
{
    "total_quarantined": 1524,
    "violation_counts": [
        {
            "rule_name":               "quantity_positive",
            "message":                 "quantity must be > 0",
            "function":                "min_value",
            "count":                   612,
            "pct_of_quarantine":       40.1,
            "sample_offending_values": {
                "event_id": ["EVT000042", ...],
                "quantity": ["-1", "-1", "0", ...],
            }
        },
        ...
    ]
}
```

#### `suggest_rules_from_quarantine(patterns, run_config_name, context, backend=None) -> list[dict]`

```
get_backend(backend)
_build_prompt(patterns, context)
llm.generate(prompt, system=_SYSTEM_PROMPT)
_parse_response(raw, run_config_name, _backend_name(llm))
```

**`_build_prompt` structure:**
```
Dataset: {context}
Total quarantined records: {N}
Top violation patterns observed in the quarantine table:

1. Rule fired: quantity_positive | Count: 612 (40.1% of quarantined records)
   Existing rule message: quantity must be > 0
   Sample quantity values when this rule fired: ['-1', '-1', '0']
   Sample event_id values when this rule fired: ['EVT000042', ...]
...

Based on these patterns, suggest NEW data quality rules that:
1. Catch additional violation sub-patterns not fully covered by existing rules
2. Detect cross-column correlations that indicate bad data
3. Identify stricter thresholds or allowlists warranted by the observed data

Do NOT duplicate the existing rules listed above.
Output a JSON array. Each rule object must have exactly these keys:
  name, column, function, constraint, criticality, message, reasoning
```

**`_parse_response`:**
- Strips markdown fences
- Extracts JSON array via regex
- Validates `_REQUIRED_KEYS = {"name", "column", "function", "constraint", "criticality", "message", "reasoning"}`
- Sets `source="quarantine_analysis"`, `llm_backend=backend_name`, `confidence=None`

**Shared with `llm_rule_suggester.py`:** Both modules use the same `_parse_response` pattern and `_REQUIRED_KEYS` set. The quarantine analyzer imports `_backend_name` from `llm_rule_suggester` to avoid duplication.

---

### 8.2 `notebooks/quarantine_analyzer.py`

**Widgets:**

| Widget | Default | Description |
|---|---|---|
| `quarantine_table` | `*.retail_dqx_poc.sales_quarantine` | Quarantine table to analyse |
| `run_config_name` | `sales_batch_rules` | Rule group for staging |
| `context_description` | `Retail POS sales transactions...` | Domain context for LLM |
| `llm_backend` | `openrouter` | LLM backend |
| `top_n` | `20` | Top N violation patterns |

**4-step execution:**
```
Step 1: df = spark.table(quarantine_table)
        patterns = extract_violation_patterns(df, top_n)      → prints violation summary

Step 2: rules = suggest_rules_from_quarantine(patterns, run_config_name, context, backend)
                                                               → prints rule count

Step 3: write_suggestions_to_staging(rules, run_config_name, spark)
                                                               → prints written count

Step 4: display(pending staging rules)
        + promotion instructions
```

---

## 9. Phase 5 — Rule Effectiveness Dashboard

### 9.1 `ai_suggester/rule_metrics.py`

#### `_build_metrics_from_rows(violation_rows, total_records, run_id, run_type, run_config_name, now) -> list[dict]`

Pure Python. Converts already-collected Spark rows into metric dicts. Extracted from `compute_rule_metrics` specifically to allow unit testing without PySpark.

```python
[
    {
        "run_id":          run_id,
        "run_type":        run_type,        # "batch" or "streaming"
        "run_config_name": run_config_name,
        "rule_name":       row["rule_name"],
        "function":        row["function"],
        "violations":      row["violations"],
        "total_records":   total_records,
        "violation_pct":   round(row["violations"] / total_records * 100, 2) if total_records > 0 else 0.0,
        "recorded_at":     now,
    }
    for row in violation_rows
]
```

#### `compute_rule_metrics(quarantine_df, total_records, run_id, run_type, run_config_name) -> list[dict]`

```python
if quarantine_df.rdd.isEmpty():
    return []

from pyspark.sql import functions as F   # deferred — not called in empty case

violation_rows = (
    quarantine_df
    .select(F.explode("_errors").alias("_err"))
    .groupBy("_err.name", "_err.function")
    .agg(F.count("*").alias("violations"))
    .collect()
)

return _build_metrics_from_rows(violation_rows, total_records, run_id, run_type, run_config_name, now)
```

**Integration points:**
- `batch_dqx_validation.py`: called after quarantine split, before Delta write
- `kafka_stream_dqx_validation.py` → `process()`: called per micro-batch

#### `write_rule_metrics(metrics, spark) -> int`

```python
if not metrics:
    return 0
ensure_metrics_table(spark)              # CREATE TABLE IF NOT EXISTS
spark.createDataFrame(rows, schema)
     .write.format("delta").mode("append")
     .saveAsTable(TABLE_RULE_METRICS)
return len(metrics)
```

---

### 9.2 `notebooks/rule_effectiveness_report.py`

**Widgets:**

| Widget | Default | Options |
|---|---|---|
| `run_config_name` | `sales_batch_rules` | `sales_batch_rules`, `streaming_event_rules`, `all` |
| `lookback_runs` | `10` | Numeric — controls zero-fire window |

**4 display sections:**

**Panel 1 — Effectiveness Summary**
```sql
SELECT rule_name, function, run_config_name,
       COUNT(DISTINCT run_id) AS run_count,
       SUM(violations) AS total_violations,
       ROUND(AVG(violation_pct), 2) AS avg_violation_pct,
       MAX_BY(violations, recorded_at) AS last_run_violations,
       MAX(recorded_at) AS last_seen
FROM dq_rule_metrics WHERE {filter}
GROUP BY rule_name, function, run_config_name
ORDER BY total_violations DESC
```

**Panel 2 — Violation Trend**
```sql
SELECT rule_name, run_type, violations, violation_pct, recorded_at
FROM dq_rule_metrics WHERE {filter}
ORDER BY recorded_at, rule_name
```
Intended for Databricks line chart: X=`recorded_at`, Y=`violations`, group=`rule_name`.

**Panel 3 — Top Offenders**
```sql
SELECT rule_name, function, SUM(violations) AS total_violations, ...
FROM dq_rule_metrics WHERE {filter}
GROUP BY rule_name, function
ORDER BY total_violations DESC LIMIT 10
```

**Panel 4 — Zero-fire Candidates**
```sql
-- Step 1: get most recent N distinct run_ids
SELECT run_id FROM dq_rule_metrics WHERE {filter}
GROUP BY run_id ORDER BY MAX(recorded_at) DESC LIMIT {lookback_runs}

-- Step 2: LEFT JOIN registry against rules that fired in those runs
SELECT r.name, r.run_config_name, r.criticality, r.check.function
FROM dq_rules_registry r
LEFT JOIN (SELECT DISTINCT rule_name FROM dq_rule_metrics WHERE run_id IN (...)) m
  ON r.name = m.rule_name
WHERE r.run_config_name = '{run_config_name}' AND m.rule_name IS NULL
```

**Fix applied:** `SELECT DISTINCT ... ORDER BY MAX(...)` is invalid SQL (DISTINCT projects only listed columns). Replaced with `GROUP BY ... ORDER BY MAX(recorded_at) DESC` which allows aggregate functions in `ORDER BY`.

---

## 10. Stage A — Data Quality Alerting

### 10.1 `ai_suggester/alerting.py`

**Purpose:** Detects rules whose `violation_pct` has spiked above their own historical baseline and notifies a Slack channel via Incoming Webhook. Runs after the Rule Effectiveness Dashboard step in the batch DAG, and on its own 15-minute cron for streaming.

**Key functions:**

```python
def detect_spikes(spark, run_config_name: str, lookback_runs: int = ALERT_LOOKBACK_RUNS) -> list[dict]
```
Compares each rule's most recent `violation_pct` (from `dq_rule_metrics`) against the average of its prior `lookback_runs` (baseline). A rule is a "spike" if:
```
last_run_pct > GREATEST(baseline_pct * ALERT_SPIKE_MULTIPLIER, ALERT_MIN_ABSOLUTE_PCT)
```
and the rule has at least `ALERT_MIN_BASELINE_RUNS` prior runs, and the run hasn't already been logged in `dq_alert_log` (dedupe via `LEFT JOIN ... WHERE alert_id IS NULL`).

```python
def format_slack_message(spikes: list[dict]) -> dict
```
Pure Python — builds a Slack Block Kit payload (`header` + `section` blocks) listing each spike's rule name, run config, last-run % vs. baseline %, and multiplier. No Spark dependency, unit-testable directly.

```python
def send_slack_alert(payload: dict, webhook_url: str) -> tuple[bool, str | None]
```
POSTs to a Slack Incoming Webhook via `requests`. Returns `(success, error_message)`.

```python
def write_alert_log(spikes: list[dict], notified: bool, error: str | None, spark) -> int
```
Appends one `dq_alert_log` row per spike (audit trail + source of the dedupe join in `detect_spikes`).

```python
def run_alert_check(spark, run_config_name: str, webhook_url: str, lookback_runs: int = ALERT_LOOKBACK_RUNS) -> list[dict]
```
Orchestrates the full check: `detect_spikes()` → if none, return `[]`; if `webhook_url` is empty, log spikes with `notified=False` and skip Slack; otherwise format + send + log. Returns the spikes processed either way.

**Alerting constants** (`config/constants.py`):
```python
TABLE_ALERT_LOG          = f"{CATALOG}.{DATABASE}.dq_alert_log"
ALERT_LOOKBACK_RUNS      = 10    # runs used to compute a rule's baseline violation_pct
ALERT_SPIKE_MULTIPLIER   = 2.5   # last run must exceed baseline_pct * this to be a spike
ALERT_MIN_ABSOLUTE_PCT   = 5.0   # floor so a near-zero baseline doesn't trivially "spike"
ALERT_MIN_BASELINE_RUNS  = 3     # don't alert until a rule has at least this many prior runs
```

### 10.2 `notebooks/dq_alert_check.py`

**Widgets:**

| Widget | Default | Description |
|---|---|---|
| `run_config_name` | `sales_batch_rules` | dropdown: `sales_batch_rules` \| `streaming_event_rules` |
| `lookback_runs` | `10` | Runs used to compute baseline |
| `secret_scope` | `dqx-poc` | Databricks secret scope holding the Slack webhook URL |

**Execution flow:**
```
Step 1: webhook_url = dbutils.secrets.get(scope=secret_scope, key="slack_webhook_url")
        (falls back to "" and continues — spikes still detected/logged, no Slack send)

Step 2: spikes = run_alert_check(spark, run_config_name, webhook_url, lookback_runs)
        prints each spike's rule_name, last_run_pct vs. baseline_pct

Step 3: display(SELECT * FROM dq_alert_log WHERE run_config_name = '{run_config_name}'
                 ORDER BY triggered_at DESC LIMIT 50)
```

**Required cluster secret setup:**
```bash
databricks secrets create-scope dqx-poc
databricks secrets put-secret dqx-poc slack_webhook_url
```

### 10.3 Native SQL Alert alternative

Databricks SQL Alerts can query `dq_rule_metrics` directly (e.g. `violation_pct > threshold` on the latest run) as a no-code alternative to the Slack-webhook path above, using Databricks' built-in alert notification channels (email, Slack app, PagerDuty, webhook). This is documented as an alternative delivery mechanism in the management doc (§10) — the `ai_suggester/alerting.py` path is the one with baseline/spike-detection logic and an audit trail (`dq_alert_log`); a native SQL Alert is simpler but only supports static thresholds, not the rolling-baseline comparison.

---

## 11. Delta Table Schemas

### `dq_rules_registry`
DQX native nested schema (managed by `promote_rule()`):
```sql
name                  STRING
criticality           STRING           -- "error" | "warning"
check                 STRUCT<
    function              STRING,      -- "is_not_null" | "min_value" | "max_value" |
                                       -- "sql_expression" | "matches_regex" | "is_in_list"
    for_each_column       ARRAY<STRING>,
    arguments             MAP<STRING, STRING>
>
filter                STRING           -- optional row filter (NULL for all rows)
run_config_name       STRING           -- "sales_batch_rules" | "streaming_event_rules"
user_metadata         MAP<STRING, STRING>  -- {"message": "..."}
created_at            TIMESTAMP
rule_fingerprint      STRING           -- NULL for promoted AI rules
rule_set_fingerprint  STRING           -- NULL for promoted AI rules
```

### `dq_rules_staging`
```sql
rule_id          STRING       -- UUID generated at suggestion time
name             STRING
column_name      STRING
function         STRING
constraint       STRING       -- JSON-serialised constraint dict
criticality      STRING
message          STRING
run_config_name  STRING
source           STRING       -- "statistical" | "llm" | "quarantine_analysis"
llm_backend      STRING       -- "claude" | "azureopenai" | "openrouter" | NULL
confidence       DOUBLE       -- 0.0–1.0 for statistical; NULL for LLM
reasoning        STRING
status           STRING       -- "pending" | "approved" | "rejected"
suggested_at     TIMESTAMP
reviewed_at      TIMESTAMP
reviewed_by      STRING
```

### `dq_rule_metrics`
```sql
run_id           STRING       -- UUID (batch) or "stream_{batch_id}_{timestamp}"
run_type         STRING       -- "batch" | "streaming"
run_config_name  STRING
rule_name        STRING
function         STRING
violations       LONG         -- count of records violating this rule in this run
total_records    LONG         -- total records processed (clean + quarantine)
violation_pct    DOUBLE       -- violations / total_records * 100
recorded_at      TIMESTAMP
```

### `stream_batch_metrics`
```sql
batch_id         INTEGER      -- Spark micro-batch ID
clean_count      LONG
quarantine_count LONG
total_count      LONG
processed_at     TIMESTAMP
```

### `dq_alert_log`
```sql
alert_id         STRING       -- UUID, one row per (rule, run) spike
rule_name        STRING
run_config_name  STRING
run_id           STRING       -- the run_id whose spike triggered this alert
baseline_pct     DOUBLE       -- average violation_pct over the lookback window
last_run_pct     DOUBLE       -- violation_pct of the run that triggered the alert
multiplier_used  DOUBLE       -- ALERT_SPIKE_MULTIPLIER at time of trigger
triggered_at     TIMESTAMP
notified         BOOLEAN      -- true if the Slack webhook POST succeeded
notify_error     STRING       -- error message if notified = false; NULL otherwise
```

### `sales_staging`, `sales_clean`, `sales_quarantine`, `stream_events_clean`, `stream_events_quarantine`
Schema inferred from source data. Quarantine tables include additional DQX columns:
- `_errors: ARRAY<STRUCT<name, message, function, columns, run_time, run_id, user_metadata MAP<STRING,STRING>, rule_fingerprint, rule_set_fingerprint, skipped>>`
- `_warnings: ARRAY<STRUCT<...>>` (same structure)

---

## 12. Cross-Module Dependency Graph

```
notebooks/batch_dqx_validation.py
    ├── databricks.labs.dqx.engine.DQEngine
    ├── databricks.labs.dqx.config.TableChecksStorageConfig
    ├── config.constants  (TABLE_RULES, TABLE_STAGING, TABLE_SALES_CLEAN,
    │                      TABLE_SALES_QUARANTINE, BATCH_RULE_CONFIG_NAME)
    ├── src.logger.get_logger
    └── ai_suggester.rule_metrics  (compute_rule_metrics, write_rule_metrics)

notebooks/kafka_stream_dqx_validation.py
    ├── databricks.labs.dqx.engine.DQEngine
    ├── rtm_kafka.config.kafka_config  (KAFKA_SPARK_OPTIONS)
    ├── config.constants  (TABLE_STREAM_CLEAN, TABLE_STREAM_QUARANTINE,
    │                      TABLE_BATCH_METRICS, STREAMING_RULE_CONFIG_NAME,
    │                      CHECKPOINT_KAFKA, STREAMING_TRIGGER_INTERVAL)
    ├── src.logger.get_logger
    └── ai_suggester.rule_metrics  (compute_rule_metrics, write_rule_metrics)

rtm_kafka.config.kafka_config
    ├── config.settings.Settings.from_env()
    └── dotenv.load_dotenv

rtm_kafka.producer.kafka_producer
    ├── confluent_kafka.Producer
    ├── rtm_kafka.config.kafka_config  (KAFKA_PRODUCER_CONFIG, TOPIC)
    ├── rtm_kafka.producer.event_generator  (generate_event)
    ├── config.constants  (PRODUCER_BATCH_SIZE, PRODUCER_INTERVAL_SECS)
    └── src.logger.get_logger

rtm_kafka.producer.event_generator
    └── config.constants  (BAD_DATA_RATE_*, ALL_DIVISIONS)

ai_suggester.profiler
    ├── pyspark.sql.functions  (deferred import)
    └── config.constants  (PROFILER_SAMPLE_SIZE, STAT_DISTINCT_THRESHOLD)

ai_suggester.rule_generator
    └── config.constants  (STAT_NULL_THRESHOLD, STAT_DISTINCT_THRESHOLD,
                           STAT_COVERAGE_THRESHOLD)

ai_suggester.llm_backend
    ├── anthropic  (ClaudeBackend — deferred)
    └── openai     (AzureOpenAIBackend, OpenRouterBackend — deferred)

ai_suggester.llm_rule_suggester
    └── ai_suggester.llm_backend  (get_backend, LLMBackend, _backend_name)

ai_suggester.quarantine_analyzer
    ├── ai_suggester.llm_backend  (get_backend)
    └── ai_suggester.llm_rule_suggester  (_backend_name)

ai_suggester.rule_writer
    └── config.constants  (TABLE_RULES_STAGING, TABLE_RULES)

ai_suggester.rule_metrics
    └── config.constants  (TABLE_RULE_METRICS)

ai_suggester.alerting
    ├── requests  (send_slack_alert — deferred import)
    ├── pyspark.sql.types  (write_alert_log — deferred import)
    ├── config.constants  (TABLE_ALERT_LOG, TABLE_RULE_METRICS, ALERT_LOOKBACK_RUNS,
    │                      ALERT_SPIKE_MULTIPLIER, ALERT_MIN_ABSOLUTE_PCT,
    │                      ALERT_MIN_BASELINE_RUNS)
    └── src.logger.get_logger

notebooks/dq_alert_check.py
    ├── ai_suggester.alerting  (run_alert_check)
    ├── config.constants  (TABLE_ALERT_LOG)
    └── dbutils.secrets  (slack_webhook_url from secret_scope)

notebooks/ai_rule_suggester.py
    ├── ai_suggester.profiler  (profile_dataframe)
    ├── ai_suggester.rule_generator  (suggest_statistical_rules)
    ├── ai_suggester.llm_rule_suggester  (suggest_llm_rules)
    ├── ai_suggester.rule_writer  (write_suggestions_to_staging)
    └── config.constants  (TABLE_RULES_STAGING)

notebooks/quarantine_analyzer.py
    ├── ai_suggester.quarantine_analyzer  (extract_violation_patterns,
    │                                      suggest_rules_from_quarantine)
    ├── ai_suggester.rule_writer  (write_suggestions_to_staging)
    └── config.constants  (TABLE_RULES_STAGING)
```

---

## 13. Rule Lifecycle State Machine

```
                    ┌─────────────────────────────────────────────────────┐
                    │              RULE CREATION SOURCES                  │
                    │                                                      │
                    │  suggest_statistical_rules()  suggest_llm_rules()   │
                    │  source="statistical"         source="llm"          │
                    │                                                      │
                    │  suggest_rules_from_quarantine()                    │
                    │  source="quarantine_analysis"                       │
                    └───────────────────┬─────────────────────────────────┘
                                        │ write_suggestions_to_staging()
                                        │ status="pending"
                                        ▼
                               ┌─────────────────┐
                               │    PENDING       │
                               │  dq_rules_       │◄──── can be reset
                               │  staging         │      to pending if
                               └────────┬─────────┘      re-review needed
                                        │
                              human review decision
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
           ┌─────────────────┐                   ┌─────────────────┐
           │    APPROVED      │                   │    REJECTED     │
           │  UPDATE staging  │                   │  UPDATE staging │
           │  status=approved │                   │  status=rejected│
           │  + INSERT into   │                   │  never enforced │
           │  dq_rules_registry                   │                 │
           └────────┬─────────┘                   └─────────────────┘
                    │
                    ▼
           ┌─────────────────┐
           │    LIVE          │
           │  dq_rules_       │
           │  registry        │◄── DQEngine.load_checks() reads this
           │                  │    at pipeline startup
           └─────────────────-┘
                    │
                    ▼
           enforced on next
           batch run OR next
           consumer restart
```

---

## 14. Test Coverage

### Test Files and Coverage

| File | Tests | Requires PySpark | Key Scenarios |
|---|---|---|---|
| `tests/test_profiler.py` | 10 | Yes (local session) | Column coverage, null%, numeric stats, distinct values, complex type handling, metadata column skipping |
| `tests/test_rule_generator.py` | 23 | No | is_not_null threshold, allowlist conditions, min/max rules, `_round_up_ceiling` (parametrised), source/key validation |
| `tests/test_quarantine_analyzer.py` | 14 | No | `_build_prompt` content, `_parse_response` JSON parsing, markdown stripping, missing keys, bad criticality |
| `tests/test_rule_metrics.py` | 9 | No | `_build_metrics_from_rows` dict structure, pct calculation, zero total, `write_rule_metrics` mock |
| `tests/test_alerting.py` | 11 | No | `detect_spikes` SQL logic (mocked), `format_slack_message` content, `send_slack_alert` success/failure, `run_alert_check` orchestration |
| `tests/test_config.py` | 9 | No | `Settings.from_env()` required-field validation, defaults for optional fields |
| `tests/test_event_generator.py` | 9 | No | Schema keys, field formats/types, bad-data injection rate bounds |

**Total: 85 tests**, all passing.

### Running Tests

```bash
# All tests (requires PySpark for test_profiler.py):
pytest tests/ -v

# Skip PySpark tests (runs anywhere):
pytest tests/ -v --ignore=tests/test_profiler.py

# Single file:
pytest tests/test_rule_generator.py -v
```

**PySpark test setup (`test_profiler.py`):**
```python
pytest.importorskip("pyspark", reason="PySpark not installed — skipping profiler tests")

@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_profiler")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
```

---

## 15. Environment Variables Reference

**Required on Databricks cluster** (set via Compute → Edit → Environment Variables; values from Azure Key Vault via `{{secrets/kv-dqx-scope/SECRET-NAME}}`):

| Variable | Secret Name | Required For |
|---|---|---|
| `AZURE_STORAGE_KEY` | `AZURE-STORAGE-KEY` | All pipelines (ADLS access) |
| `EVENTHUB_CONN_STR` | `EVENTHUB-CONN-STR` | Streaming pipeline + Kafka producer |
| `KAFKA_BOOTSTRAP_SERVERS` | — | `evhns-dqx-poc.servicebus.windows.net:9093` |
| `KAFKA_TOPIC` | — | `retail-pos-stream` |
| `KAFKA_CONSUMER_GROUP` | — | `databricks-dqx-kafka-consumer` |
| `LLM_BACKEND` | — | `openrouter` (or `claude` / `azure_openai`) |
| `OPENROUTER_API_KEY` | — | AI Rule Suggester + Quarantine Analyzer (OpenRouter backend) |
| `OPENROUTER_MODEL` | — | Optional; defaults to `openrouter/auto` |
| `ANTHROPIC_API_KEY` | — | Required only if `LLM_BACKEND=claude` |
| `AZURE_OPENAI_API_KEY` | — | Required only if `LLM_BACKEND=azure_openai` |
| `AZURE_OPENAI_ENDPOINT` | — | Required only if `LLM_BACKEND=azure_openai` |
| `AZURE_OPENAI_DEPLOYMENT` | — | Required only if `LLM_BACKEND=azure_openai` |

**Required locally** (in `.env`, loaded by `load_dotenv()`):

Same as above, plus:
```
DATABRICKS_HOST=https://adb-7405619883480512.12.azuredatabricks.net
DATABRICKS_TOKEN=<personal access token>
DATABRICKS_CLUSTER_ID=0601-042329-nm42bg1s
```

---

## 16. Databricks Jobs Reference

### Job: `[RTM] Kafka Streaming Pipeline` (`rtm_kafka_streaming`)

```
Tasks (concurrent — no depends_on):
  1. stream_and_validate → notebooks/kafka_stream_dqx_validation.py
     timeout_seconds: 0   (runs indefinitely until cancelled)

  2. produce_events      → notebooks/kafka_producer_job.py
     Parameters:
       --num_events=1000
       --batch_size=10
       --interval_seconds=5
```
`produce_events` has no `depends_on` on `stream_and_validate` — a streaming task with `timeout_seconds: 0` never reaches a terminal state, so Databricks' `depends_on` would never fire. Both tasks start together; the producer polls a `stream_consumer_ready` readiness signal written by the consumer once it's actually listening, before it sends any events.

### Job: `[RTM] Batch DQX Pipeline` (`rtm_batch_pipeline`)

```
Tasks (DAG):
  1. stage_data          → notebooks/setup_delta_tables.py
     Parameters:
       --csv_path=abfss://databricks-dqx@databricksdqxpocstorage.dfs.core.windows.net/sales_transactions.csv
       --database=retail_dqx_poc

  2. validate_data       → notebooks/batch_dqx_validation.py    depends_on: stage_data
     Parameters:
       --database=retail_dqx_poc

  3. suggest_rules       → notebooks/ai_rule_suggester.py (notebook_task)   depends_on: validate_data
     base_parameters:
       source_table:        dqx_poc_cluster_7405619883480512.retail_dqx_poc.sales_staging
       run_config_name:     sales_batch_rules
       context_description: Retail POS sales transactions from brick-and-mortar stores
       llm_backend:         openrouter

  4. analyze_quarantine  → notebooks/quarantine_analyzer.py (notebook_task)   depends_on: validate_data
     base_parameters:
       quarantine_table:    dqx_poc_cluster_7405619883480512.retail_dqx_poc.sales_quarantine
       run_config_name:     sales_batch_rules
       context_description: Retail POS sales transactions from brick-and-mortar stores
       llm_backend:         openrouter
       top_n:               20

  5. run_report          → notebooks/rule_effectiveness_report.py (notebook_task)   depends_on: [suggest_rules, analyze_quarantine]
     base_parameters:
       run_config_name: sales_batch_rules
       lookback_runs:   10

  6. check_alerts        → notebooks/dq_alert_check.py (notebook_task)   depends_on: run_report
     base_parameters:
       run_config_name: sales_batch_rules
       lookback_runs:   10
       secret_scope:    dqx-poc
```
`suggest_rules` and `analyze_quarantine` both depend only on `validate_data` and run in parallel; `run_report` waits on both before running; `check_alerts` runs last.

### Job: `[RTM] DQ Alert Check (Streaming)` (`dq_alert_check_streaming`)

```
Tasks:
  1. check_alerts → notebooks/dq_alert_check.py (notebook_task)
     base_parameters:
       run_config_name: streaming_event_rules
       lookback_runs:   10
       secret_scope:    dqx-poc

Schedule: cron "0 */15 * * * ?" (every 15 minutes, UTC)
```
Runs independently of the batch DAG's `check_alerts` task since the streaming pipeline has no equivalent end-of-run hook to chain off of — it runs indefinitely.

---

## Appendix — DQX Function Types Reference

| Function | Constraint format | Example |
|---|---|---|
| `is_not_null` | `{}` | Field must not be null |
| `min_value` | `{"min": <number>}` | Value must be ≥ min |
| `max_value` | `{"max": <number>}` | Value must be ≤ max |
| `sql_expression` | `{"expression": "<SQL bool>"}` | `quantity > 0 AND unit_price > 0` |
| `matches_regex` | `{"regex": "<pattern>"}` | `^EVT[0-9]{6}$` |
| `is_in_list` | `{"allowed": [<values>]}` | Division must be in allowed list |

The `sql_expression` function is **not a native DQX built-in** — it is registered via the `custom_check_functions` dict in each validation notebook:
```python
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}
```
This enables cross-column rules and arbitrary SQL boolean expressions.
