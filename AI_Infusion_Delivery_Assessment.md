---
marp: true
theme: default
paginate: true
headingDivider: 1
style: |
  section {
    font-family: 'Segoe UI', Arial, sans-serif;
    font-size: 21px;
    padding: 38px 56px;
  }
  section.title {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
  }
  section.title h1 { font-size: 2.4em; color: #e94560; margin-bottom: 10px; }
  section.title h2 { font-size: 1.2em; color: #a8dadc; font-weight: 300; border: none; }
  section.title p  { color: #ccc; font-size: 0.85em; margin-top: 28px; }
  section.divider {
    background: linear-gradient(135deg, #0f3460 0%, #16213e 100%);
    color: white;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
  }
  section.divider h1 { font-size: 2em; color: #e94560; }
  section.divider p  { color: #a8dadc; font-size: 1.05em; }
  h1 { color: #0f3460; border-bottom: 3px solid #e94560; padding-bottom: 8px; }
  h2 { color: #16213e; border-bottom: 1px solid #ddd; }
  h3 { color: #0f3460; margin-top: 12px; margin-bottom: 4px; }
  table { width: 100%; border-collapse: collapse; font-size: 0.82em; }
  th   { background: #0f3460; color: white; padding: 7px 11px; }
  td   { padding: 7px 11px; border-bottom: 1px solid #eee; }
  tr:nth-child(even) td { background: #f7f9fc; }
  pre  { font-size: 0.72em; background: #1e1e2e; color: #cdd6f4; padding: 14px 18px;
         border-radius: 6px; border-left: 4px solid #e94560; }
  code { background: #f1f3f5; padding: 1px 5px; border-radius: 3px; font-size: 0.82em; }
  pre code { background: transparent; color: inherit; font-size: inherit; padding: 0; }
  blockquote { background: #e8f4f8; border-left: 4px solid #e94560; padding: 10px 14px;
               margin: 10px 0; border-radius: 0 4px 4px 0; font-style: normal; }
  .cols { display: grid; grid-template-columns: 1fr 1fr; gap: 28px; }
  footer { font-size: 0.65em; color: #aaa; }
---

<!-- _class: title -->

# AI-Infused Data Quality Platform

## Proof of Concept — Retail & Wholesale Operations

---

**Assessment: AI Infusion in Delivery**
Databricks · Delta Lake · Apache Kafka · LLM-Powered Rule Generation

---

# Agenda

1. **The Problem** — Data quality at scale in retail operations
2. **The Vision** — What AI-infused data quality looks like
3. **What We Built** — POC overview and technology stack
4. **Phase-by-Phase Walkthrough** — Five working components
5. **AI in Action** — Where and how AI is infused
6. **Architecture Deep Dive** — How it all connects
7. **Production Considerations** — From POC to enterprise scale
8. **What We Proved** — Key POC outcomes
9. **AI Infusion Opportunities** — What comes next

---

<!-- _class: divider -->

# Part 1: The Problem

*Why data quality in retail needs to be smarter*

---

# The Data Quality Challenge in Retail & Wholesale

### Every day, bad data costs the business

| Data Problem | Business Impact |
|---|---|
| Incorrect pricing records | Revenue leakage, pricing disputes |
| Missing inventory quantities | Stockouts or overstock decisions |
| Duplicate transaction records | Double-counting in financial reports |
| Stale / out-of-order event streams | Wrong demand-forecasting inputs |
| Corrupted supplier data | Bad purchase order generation |

<br>

> **The hidden cost:** Most bad data is discovered *after* it has already influenced a decision — by the time it surfaces, the damage is done.

---

# Why Traditional DQ Approaches Break Down

```
Manual Rule Writing  →  Slow to create, slow to update, misses new patterns
```

**Today's reality in most data teams:**

- A data engineer **manually writes** each validation rule
- Rules are written *after* a data problem is discovered — reactive, not proactive
- Rules are often **never reviewed** once written — stale rules pile up silently
- When source schemas change, rules **silently become irrelevant**
- No visibility into *which rules are actually catching anything*
- Streaming data adds real-time complexity that batch-only rules can't address

> **The gap:** Data volumes and schema complexity are growing faster than a team can write and maintain rules by hand.

---

# What We Need

```
              TODAY                              TARGET
    ┌──────────────────────┐         ┌──────────────────────────────┐
    │  Data Engineer       │         │  AI analyses data profile    │
    │  writes rules        │  →→→→   │  suggests rules              │
    │  manually, reactively│         │  learns from violations      │
    │  no effectiveness    │         │  continuously self-improves  │
    │  feedback            │         │  governed by humans          │
    └──────────────────────┘         └──────────────────────────────┘

       Reactive, slow, brittle          Proactive, adaptive, governed
```

**Three shifts required:**
1. **Batch → Real-time** — Catch bad data as it arrives, not the next morning
2. **Manual → AI-Generated** — Let AI analyse the data and write the rules
3. **Static → Self-Improving** — Rules that evolve as data patterns change

---

<!-- _class: divider -->

# Part 2: The Vision

*An AI-infused system that writes, validates, and improves its own DQ rules*

---

# The AI Infusion Vision

```
┌────────────────────────────────────────────────────────────────────────┐
│                       CONTINUOUS FEEDBACK LOOP                          │
│                                                                          │
│   Raw Data ──► DQ Validation ──► Quarantine Bad Records                 │
│       ▲              │                     │                            │
│       │              │                     ▼                            │
│  Rules Registry ◄── AI Rule ◄── AI Analyses Patterns                   │
│  (promoted rules)   Suggester     in bad records                        │
│       │                                    │                            │
│       │             Rule Effectiveness ◄───┘                            │
│       └─────────────  Dashboard                                         │
│                    (which rules fire, which don't)                      │
└────────────────────────────────────────────────────────────────────────┘
```

**AI is not a bolt-on — it is the feedback mechanism that makes the system self-improving.**

---

<!-- _class: divider -->

# Part 3: What We Built

*A working, end-to-end POC on Databricks*

---

# POC Overview — Five Working Phases

| # | Phase | What It Does | Status |
|---|---|---|---|
| 1 | **Batch DQ Validation** | Validates daily transaction / product data against rules | ✅ Complete |
| 2 | **Real-time Streaming Validation** | Validates live event streams via Kafka / Event Hubs | ✅ Complete |
| 3 | **AI Rule Suggester** | LLM analyses data profile and generates DQ rules | ✅ Complete |
| 4 | **Quarantine Analyzer** | Surfaces patterns in bad records, root-cause insights | ✅ Complete |
| 5 | **Rule Effectiveness Dashboard** | Tracks which rules fire, trends, zero-fire candidates | ✅ Complete |

<br>

> **Current state:** All five phases are built, integrated, and running on Databricks. The POC uses generated retail data (sales transactions + event streams) designed to mirror real-world data patterns at scale.

---

# Technology Stack

<div class="cols">

**Platform & Data**
- **Databricks** — unified compute, notebooks, jobs
- **Delta Lake** — ACID-compliant storage for all DQ data
- **Apache Kafka / Azure Event Hubs** — real-time event streaming
- **Unity Catalog** — governed data asset management
- **PySpark** — distributed data processing

**AI & Quality Framework**
- **Databricks Labs DQX** — core DQ rule engine
- **Anthropic Claude** — primary LLM for rule generation
- **Azure OpenAI** — enterprise LLM alternative (pluggable)
- **OpenRouter** — multi-model fallback option
- **Databricks Asset Bundles** — infra-as-code for all jobs

</div>

> **Key design decision:** The LLM backend is pluggable — switching between Claude, Azure OpenAI, or any other model requires only an environment variable change, not code changes.

---

<!-- _class: divider -->

# Part 4: Phase-by-Phase Walkthrough

---

# Phase 1 — Batch Data Quality Validation

**Validates daily sales / inventory datasets against a governed rule set.**

```python
# notebooks/batch_dqx_validation.py
dq_df = dqx.apply_checks_by_metadata(
    sales_df, checks,
    custom_check_functions=custom_check_functions,
)

# Split clean vs. quarantine on the _errors array DQX attaches to each row
good_df       = dq_df.filter((col("_errors").isNull()) | (size(col("_errors")) == 0))
quarantine_df = dq_df.filter((col("_errors").isNotNull()) & (size(col("_errors")) > 0))

# Rule effectiveness captured after every run
_run_id  = str(uuid.uuid4())
_metrics = compute_rule_metrics(quarantine_df, total_count, _run_id, "batch", BATCH_RULE_CONFIG_NAME)
_written = write_rule_metrics(_metrics, spark)
```

**Example rules in `configs/rules_batch.yml`:**
```yaml
- name: unit_price_positive
  column: unit_price
  type: min_value
  min: 0.01
  criticality: error
- name: division_allowlist
  column: division
  type: is_in_list
  allowed_values: ["011", "012", "013"]
  criticality: error
```

---

# Phase 2 — Real-time Streaming Validation

**Validates every Kafka / Azure Event Hubs event before it reaches any consumer.**

```python
# notebooks/kafka_stream_dqx_validation.py — foreachBatch handler
def process(batch_df, batch_id):
    dq_df = dqx.apply_checks_by_metadata(
        batch_df, checks,
        custom_check_functions=custom_check_functions,
    )
    # Normalise NULL arrays to keep Delta schema consistent across batches
    dq_df = dq_df.withColumn("_errors",
        expr("CASE WHEN _errors IS NULL THEN array() ELSE _errors END"))

    clean      = dq_df.filter(size(col("_errors")) == 0).drop("_errors")
    quarantine = dq_df.filter(size(col("_errors")) > 0)

    clean.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLE_STREAM_CLEAN)
    quarantine.write.mode("append").option("mergeSchema", "true").saveAsTable(TABLE_STREAM_QUARANTINE)
```

**Key design:** Rules are loaded once at stream startup — no per-batch registry reads. ADLS-backed checkpoints ensure fault tolerance and exactly-once semantics.

---

# Phase 3 — AI Rule Suggester ✨

**Profiles a data table and uses an LLM to generate new DQ rules automatically.**

```
Step 1: Profile      Step 2: Prompt           Step 3: Generate
────────────         ──────────────           ────────────────
Analyse column  ──►  Build structured    ──►  LLM returns
stats: min, max,     prompt with profile      candidate rules
nulls, cardinality,  + domain context         in JSON format
distributions
                                                     │
Step 4: Validate     Step 5: Human Review            ▼
────────────────     ─────────────────        dq_rules_staging
Parse + validate ◄── Engineer: approve ◄───── (status=pending)
rule structure       or reject
     │
     ▼
dq_rules_registry  ← promoted and active in all future runs
```

**Two complementary rule sources:** Statistical rules (no LLM, pure maths) + LLM rules (domain reasoning).

---

# Phase 3 — How the LLM Backend Works

**Three providers, one interface — switch via environment variable:**

```python
# ai_suggester/llm_backend.py
def get_backend(name: str | None = None) -> LLMBackend:
    resolved = (name or os.environ.get("LLM_BACKEND", "claude")).lower()
    if resolved == "claude":        return ClaudeBackend()        # Anthropic API
    if resolved == "azure_openai":  return AzureOpenAIBackend()   # Azure OpenAI
    if resolved == "openrouter":    return OpenRouterBackend()     # OpenRouter

# ClaudeBackend.generate() — actual call
message = client.messages.create(
    model=self._model,          # e.g. "claude-sonnet-4-6"
    max_tokens=4096,
    system=_SYSTEM_PROMPT,      # "You are a senior data quality engineer..."
    messages=[{"role": "user", "content": prompt}],
)
```

**The LLM is given a strict system prompt** that specifies exact output format (JSON array), allowed functions (`is_not_null`, `min_value`, `max_value`, `sql_expression`, `matches_regex`, `is_in_list`), and required fields — so the output is always machine-parseable.

---

# Phase 3 — Statistical Rule Generator (No LLM)

**Derives rules purely from column statistics — fast, deterministic, no API cost:**

```python
# ai_suggester/rule_generator.py — suggest_statistical_rules()
for col_name, stats in profile.items():
    null_pct = stats.get("null_pct", 1.0)

    # Rule: is_not_null — if < 1% nulls observed historically
    if null_pct < STAT_NULL_THRESHOLD:   # STAT_NULL_THRESHOLD = 0.01
        rules.append({"name": f"{col_name}_not_null",
                       "function": "is_not_null", "confidence": 1.0 - null_pct, ...})

    # Rule: min_value — if all observed values are strictly positive
    if is_numeric and col_min is not None and col_min > 0:
        rules.append({"name": f"{col_name}_min_value",
                       "function": "min_value", "constraint": {"min": 0}, ...})

    # Rule: is_in_list — for low-cardinality categoricals (≤ 50 distinct values)
    if distinct_count <= STAT_DISTINCT_THRESHOLD and not is_numeric:
        rules.append({"name": f"{col_name}_valid_values",
                       "function": "sql_expression", ...})
```

Statistical rules provide a **fast baseline** before the LLM adds domain-reasoning rules on top.

---

# Phase 3 — Human Review & Promotion

**No AI rule becomes active without engineer sign-off:**

```python
# ai_suggester/rule_writer.py — staging schema
_STAGING_DDL = """
CREATE TABLE IF NOT EXISTS dq_rules_staging (
    rule_id         STRING,    -- UUID per suggestion
    name            STRING,    -- e.g. "unit_price_positive"
    function        STRING,    -- "min_value", "is_not_null", etc.
    criticality     STRING,    -- "error" | "warning"
    source          STRING,    -- "statistical" | "llm"
    llm_backend     STRING,    -- "claude" | "azure_openai" | null
    confidence      DOUBLE,    -- 0.0–1.0
    reasoning       STRING,    -- LLM's explanation
    status          STRING,    -- "pending" → "approved" | "rejected"
    suggested_at    TIMESTAMP,
    reviewed_at     TIMESTAMP,
    reviewed_by     STRING
) USING DELTA
"""
# Engineer calls: approve_rule(rule_id, spark) → inserts into dq_rules_registry
#                 reject_rule(rule_id, spark)  → marks status="rejected", preserved
```

---

# Phase 4 — Quarantine Analyzer

**Turns a table of bad records into actionable data quality incidents:**

```
  dq_sales_quarantine / stream_events_quarantine
                │
                ▼
  Quarantine Analyzer Notebook
  ┌──────────────────────────────────────────────────────┐
  │  • Which rules are triggering most violations?        │
  │  • Are violations concentrated in specific stores?    │
  │  • Are violations correlated with time? (weekends?)   │
  │  • Which columns appear in the most violation records?│
  └──────────────────────────────────────────────────────┘
                │
                ▼
  Example insight: "60% of price violations originate
  from division '540' — a division flagged in the
  coupon_division_integrity rule"
```

**Powered by the `_errors` column** that DQX attaches to every quarantined record:
```
_errors: ARRAY<STRUCT<name STRING, message STRING, function STRING, columns ARRAY<STRING>>>
```
No separate error log table needed — the error context travels with each bad record.

---

# Phase 5 — Rule Effectiveness Dashboard

**Tracks per-rule violation counts after every batch and stream run:**

```python
# ai_suggester/rule_metrics.py — called after every run
violation_rows = (
    quarantine_df
    .select(F.explode("_errors").alias("_err"))
    .groupBy(
        F.col("_err.name").alias("rule_name"),
        F.col("_err.function").alias("function"),
    )
    .agg(F.count("*").alias("violations"))
    .collect()
)
# violation_pct = violations / total_records * 100
# Appended to dq_rule_metrics (Delta) — full run history preserved
```

**Four report sections in `notebooks/rule_effectiveness_report.py`:**

```sql
-- Effectiveness Summary (actual query)
SELECT rule_name, COUNT(DISTINCT run_id) AS run_count,
       SUM(violations) AS total_violations,
       ROUND(AVG(violation_pct), 2) AS avg_violation_pct,
       MAX_BY(violations, recorded_at) AS last_run_violations
FROM dq_rule_metrics
GROUP BY rule_name, function, run_config_name
ORDER BY total_violations DESC
```

---

<!-- _class: divider -->

# Part 5: AI in Action

*Where intelligence is infused — and what business problem each solves*

---

# AI Infusion Map — Current State

| AI Component | Module | What It Does | Business Use Case |
|---|---|---|---|
| **Statistical Rule Generator** | `rule_generator.py` | Derives rules from null %, min/max, cardinality | Fast baseline rules from data alone — no LLM cost |
| **LLM Rule Generator** | `llm_rule_suggester.py` | LLM reasons over profile, adds domain rules | Catch semantic violations statistics can't see |
| **Pluggable LLM Backend** | `llm_backend.py` | Claude / Azure OpenAI / OpenRouter — env var switch | Enterprise flexibility, no vendor lock-in |
| **Structured Prompt Engine** | `llm_rule_suggester.py` | Converts column stats into strict-format LLM prompt | Machine-parseable JSON output every time |
| **Rule Validation Layer** | `llm_rule_suggester.py` | Validates LLM JSON before staging | Prevents malformed AI rules entering the system |
| **Quarantine Pattern Analyser** | `quarantine_analyzer.py` | Finds violation clusters in bad records | Prioritise which data issues to fix first |
| **Zero-fire Detection** | `rule_effectiveness_report.py` | Flags rules with no violations in last N runs | Identifies stale rules — candidates for AI refresh |

---

# The Human-in-the-Loop Design

**AI generates. Humans govern. System learns.**

```
                  ┌──────────────────┐
  AI Suggests ──► │  dq_rules_       │ ──► Engineer reviews
  (statistical    │  staging         │     approve / reject
   + LLM rules)   │  status=pending  │          │
                  └──────────────────┘          │
                                                ▼
                                       ┌──────────────────┐
                                       │  dq_rules_       │
                                       │  registry        │  ← single source of truth
                                       │  status=approved │    for batch + streaming
                                       └──────────────────┘
```

**Why this matters architecturally:**
- Full audit trail: `rule_id`, `suggested_at`, `reviewed_at`, `reviewed_by`, `llm_backend`
- Rejected rules are preserved with reason — future fine-tuning data
- The staging → registry promotion is identical to a **feature-flag promotion workflow**
- Zero risk of a hallucinated rule silently entering production

---

<!-- _class: divider -->

# Part 6: Architecture Deep Dive

---

# Full System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       DATABRICKS UNITY CATALOG                           │
│   Catalog: dqx_poc_dev / DB: retail_dqx_poc        │
│                                                                           │
│  ┌──────────────┐  ┌───────────────────────────────────────────────┐    │
│  │ Config Layer │  │               PROCESSING LAYER                 │    │
│  │ constants.py │─►│  Batch Pipeline          Streaming Pipeline    │    │
│  │ rules_*.yml  │  │  batch_dqx_              kafka_stream_dqx_    │    │
│  │ settings.py  │  │  validation.py           validation.py        │    │
│  └──────────────┘  │         │  (foreachBatch)         │           │    │
│                    │         └──────────┬───────────────┘           │    │
│  ┌──────────────┐  │                   ▼                            │    │
│  │  AI Layer    │  │          DQX Rule Engine                       │    │
│  │  profiler    │  │     apply_checks_by_metadata()                 │    │
│  │  rule_gen    │─►│                   │                            │    │
│  │  llm_backend │  │       ┌───────────┴───────────┐               │    │
│  │  rule_writer │  │       ▼                       ▼               │    │
│  └──────────────┘  │  sales_clean /         sales_quarantine /     │    │
│                    │  stream_events_clean   stream_events_quarantine│    │
│  ┌──────────────┐  │                               │               │    │
│  │ Metrics Layer│◄─│                        dq_rule_metrics        │    │
│  │ rule_metrics │  │                               │               │    │
│  └──────────────┘  │                        dq_rules_staging       │    │
│                    │                        dq_rules_registry      │    │
│                    └───────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
        ▲                                                    ▲
  Azure Event Hubs                                    LLM APIs
  (Kafka protocol)                             (Claude / Azure OpenAI)
```

---

# Delta Table Data Model

```python
# config/constants.py — single source of truth for all table names
CATALOG  = "dqx_poc_dev"
DATABASE = "retail_dqx_poc"

TABLE_SALES_CLEAN       = f"{CATALOG}.{DATABASE}.sales_clean"
TABLE_SALES_QUARANTINE  = f"{CATALOG}.{DATABASE}.sales_quarantine"
TABLE_STREAM_CLEAN      = f"{CATALOG}.{DATABASE}.stream_events_clean"
TABLE_STREAM_QUARANTINE = f"{CATALOG}.{DATABASE}.stream_events_quarantine"
TABLE_RULES             = f"{CATALOG}.{DATABASE}.dq_rules_registry"
TABLE_RULES_STAGING     = f"{CATALOG}.{DATABASE}.dq_rules_staging"
TABLE_RULE_METRICS      = f"{CATALOG}.{DATABASE}.dq_rule_metrics"
```

| Table | Key Columns | Written By |
|---|---|---|
| `sales_clean` | all source columns | Batch pipeline |
| `sales_quarantine` | source + `_errors ARRAY<STRUCT>` | Batch pipeline |
| `stream_events_clean` | all source columns | Streaming pipeline |
| `stream_events_quarantine` | source + `_errors ARRAY<STRUCT>` | Streaming pipeline |
| `dq_rules_registry` | `name, check STRUCT, criticality` | rule_writer.promote_rule() |
| `dq_rules_staging` | + `status, llm_backend, confidence, reasoning` | AI Rule Suggester |
| `dq_rule_metrics` | `run_id, rule_name, violations, violation_pct` | After every run |

---

# Databricks Jobs — Operational View

| Job Name | Trigger | Notebook / Script |
|---|---|---|
| `[RTM] Setup Delta Tables` | Once (infra) | `notebooks/setup_delta_tables.py` |
| `[RTM] Batch DQX Validation` | Scheduled / on-demand | `notebooks/batch_dqx_validation.py` |
| `[RTM] Kafka Stream DQX Validation` | Continuous | `notebooks/kafka_stream_dqx_validation.py` |
| `[RTM] AI Rule Suggester` | On-demand | `notebooks/ai_rule_suggester.py` |
| `[RTM] Quarantine Analyzer` | On-demand / scheduled | `notebooks/quarantine_analyzer.py` |
| `[RTM] Rule Effectiveness Report` | On-demand / scheduled | `notebooks/rule_effectiveness_report.py` |

All jobs defined in `databricks.yml` as a **Databricks Asset Bundle** — infrastructure as code, deployable across dev / staging / prod with a single CLI command: `databricks bundle deploy`.

---

<!-- _class: divider -->

# Part 7: Production Considerations

*From POC to enterprise scale*

---

# POC vs. Production — Honest Assessment

| Dimension | POC (Current State) | Production Path |
|---|---|---|
| **Data** | Generated synthetic retail data | Real transaction / event feeds from source systems |
| **Scale** | Thousands of records | Billions of records — Spark scales horizontally, no code changes |
| **Rules** | ~15 seeded + AI-suggested | Hundreds of rules across dozens of datasets |
| **Streaming** | Single Kafka topic | Multiple topics, partitioned, multi-tenant |
| **LLM Cost** | Low (POC usage) | Prompt caching implemented; cost monitoring needed |
| **Security** | Databricks Secrets | Enterprise key vault (Azure Key Vault / HashiCorp) |
| **Governance** | Unity Catalog in place | Column-level access controls + data lineage |
| **Monitoring** | Rule metrics Delta table | Connect to Datadog / Grafana / Azure Monitor |

> **Key point:** Every technology choice is production-grade. Databricks, Delta Lake, Kafka, and Unity Catalog are what we would use in production. The POC proves the *pattern* — plugging in real data is a configuration and integration exercise, not a re-architecture.

---

# Scalability by Design

```python
# The same code path runs on thousands of rows today
# and billions of rows in production — Spark abstracts the scale

# Profiler respects a configurable sample ceiling to keep AI calls affordable
PROFILER_SAMPLE_SIZE = 10_000   # config/constants.py
# → profile is computed on a representative sample; rules apply to the full table

# Bad-data injection rates (synthetic data) mirror realistic production rates
BAD_DATA_RATE_QUANTITY = 0.06   # 6%  → triggers quantity_positive rule
BAD_DATA_RATE_PRICE    = 0.04   # 4%  → triggers unit_price_positive rule
BAD_DATA_RATE_EVENT_ID = 0.02   # 2%  → triggers event_id_not_null rule
```

**What changes when moving to real data:**
1. Replace synthetic data generator with real Kafka topic / batch source connector
2. Adjust rule YAML column names to match real schema
3. Run AI Rule Suggester against the real data profile — LLM generates production-relevant rules
4. Enable Unity Catalog column-level lineage and audit logging
5. Wire `dq_rule_metrics` to operational dashboards and alerting (PagerDuty / Teams)

---

<!-- _class: divider -->

# Part 8: What We Proved

---

# Key POC Outcomes

**1. AI can write meaningful DQ rules from data alone**
The LLM produced structurally valid, business-relevant rules from a column profile — no domain hints given. Rules were immediately executable by DQX with zero manual editing.

**2. The human-in-the-loop governance model works**
Staging → review → registry promotion gives engineers full control without slowing the system. The approve/reject audit trail is preserved for future model fine-tuning.

**3. Batch and streaming share a single rule set**
A rule approved in `dq_rules_registry` applies identically to daily batch runs and Kafka micro-batches — one source of truth, no duplication.

**4. Rule effectiveness tracking reveals rule health**
Zero-fire detection correctly surfaces rules that have stopped being relevant — a capability that is impossible to get from a static rules list.

**5. Pluggable LLM backend delivers enterprise flexibility**
Switching from Claude to Azure OpenAI required only setting `LLM_BACKEND=azure_openai` — no code modification. This is the architecture pattern for responsible AI adoption at enterprise scale.

---

# Test Coverage and Code Quality

**48 unit tests across 4 files — all passing locally, no Spark cluster required:**

| Test File | Tests | What Is Covered |
|---|---|---|
| `tests/test_profiler.py` | 12 | Column statistics, null detection, distribution analysis |
| `tests/test_rule_generator.py` | 18 | Prompt construction, JSON parse, rule validation |
| `tests/test_rule_metrics.py` | 9 | Metrics dict building, violation % calc, write path |
| `tests/test_rule_writer.py` | 9 | Staging insert, approve/reject lifecycle, registry promotion |

```
$ pytest tests/ -v
...
48 passed in 3.21s
```

**Design principle applied throughout:** Pure-Python business logic is extracted from Spark-dependent code, enabling fast offline tests without a running cluster. Spark interactions are mocked at the boundary using `unittest.mock`.

---

<!-- _class: divider -->

# Part 9: AI Infusion Opportunities

*Where this platform goes next*

---

# Known AI Infusion Opportunities — Roadmap

| Priority | Opportunity | AI Infusion Point | Business Value |
|---|---|---|---|
| **A** | **Automated Alerting** | AI classifies violation severity → routes alert | On-call team notified only when it matters |
| **B** | **Data Drift Detection** | ML detects statistical distribution shift | Catch silent degradation before rules fail |
| **C** | **DQ Scorecard** | Aggregate quality scores per domain / table | Executive visibility into data health by business area |
| **D** | **Rule Governance Workflow** | AI suggests rule retirement based on effectiveness | Fully automated rule lifecycle management |
| **E** | **Multi-table / Cross-domain** | Referential integrity + cross-table consistency | Catches issues single-table rules cannot see |

> **Recommended next step: A (Alerting)** — highest immediate value, lowest lift. Wires the existing `dq_rule_metrics` table to a notification layer. No new ML needed.

---

# Opportunity A — Automated Alerting

```
  dq_rule_metrics (populated after every batch / stream run)
           │
           ▼
  Alert Evaluation Engine
  ┌──────────────────────────────────────────────────────┐
  │  if violation_pct > threshold for rule X             │
  │  → classify severity (info / warning / critical)     │
  │  → deduplicate (don't re-alert same issue twice)     │
  │  → route to appropriate channel                      │
  └──────────────────────────────────────────────────────┘
           ├──► Slack / Teams notification
           ├──► PagerDuty / OpsGenie (critical only)
           └──► dq_alerts table (full audit log)
```

**AI infusion:** Use the LLM to write the alert message — not just *"rule X violated 12%"* but:
> *"Price validation for division '540' failed at 12% — this pattern has not been seen before and may indicate a feed configuration issue."*

Existing infrastructure (`dq_rule_metrics`, the LLM backend) is already in place. Alerting is an orchestration layer on top.

---

# Opportunity B — Data Drift Detection

**What it is:** Monitor column distributions run-over-run. When distributions shift significantly, flag it — even before a rule explicitly covers the new pattern.

```
  Run 1 (baseline)               Run 8 (drift detected)
  unit_price: mean=24.50   →     unit_price: mean=24.48   (OK)
  unit_price: stddev=18.2  →     unit_price: stddev=52.1  ← DRIFT ⚠️
                                                                 │
                                                                 ▼
                                          AI Rule Suggester auto-triggered
                                          to generate updated range rules
                                          for the affected column
```

**Why it matters for retail / wholesale:**
- Supplier price changes, seasonal promotions, and currency shifts alter distributions
- Current static rules (`unit_price > 0.01`) become insufficient without anyone noticing
- Drift detection catches *when the world changes*, before downstream impact materialises

**AI infusion:** Detected drift auto-triggers the AI Rule Suggester on affected columns — the system updates its own rules in response to data change, closing the feedback loop fully.

---

# The Vision, Realised

```
┌──────────────────────────────────────────────────────────────────────┐
│                                                                        │
│   TODAY (POC — 5 phases complete)    TOMORROW (Production AI Infusion)│
│   ─────────────────────────────      ────────────────────────────     │
│                                                                        │
│   ✅ AI generates DQ rules           + AI detects when rules go stale │
│   ✅ Batch + stream validated         + AI classifies alert severity   │
│   ✅ Quarantine records analysed      + AI explains root cause in eng  │
│   ✅ Rule effectiveness tracked       + AI suggests rule updates       │
│   ✅ Human governance layer           + AI scorecard for exec view     │
│                                                                        │
│   Result: A data quality platform that improves itself,                │
│   governed by humans, powered by AI, built on Databricks.             │
│                                                                        │
└──────────────────────────────────────────────────────────────────────┘
```

---

<!-- _class: divider -->

# Summary & Discussion

---

# What We Are Asking For

**Today's ask:** Validate the POC architecture and AI infusion approach with the senior technical architect.

**Discussion points:**
1. Does the human-in-the-loop governance model align with enterprise AI adoption standards?
2. Is the pluggable LLM backend pattern the right approach for multi-cloud / multi-provider deployments?
3. Which next-stage opportunity (Alerting / Drift / Scorecard) aligns best with delivery priorities?
4. What production integration patterns (source systems, security, monitoring) should shape the next phase?

<br>

> **The POC proves the pattern works.** Every component is built, tested, and running on Databricks. The question is no longer *can we do this* — it is *which direction do we take it next.*

---

<!-- _class: title -->

# Thank You

## Questions & Discussion

---

**Databricks DQX POC — AI Infusion in Delivery**

*5 phases complete · 48 unit tests passing · Production-grade architecture · Ready for next phase planning*

---
