# Data Quality Framework — POC Documentation

> **Audience:** Technical leads and upper management
> **Purpose:** Current state overview, run guide, and roadmap
> **Last updated:** June 2026

---

## 1. What Is This POC?

This proof-of-concept evaluates **Databricks Labs DQX** as a centralised, AI-assisted data quality framework for retail POS data pipelines.

The core idea: instead of hardcoding data quality rules inside individual pipelines (which breaks the moment data changes), all rules live in a **single governed table**. Every pipeline reads from that table at runtime. Adding, changing, or retiring a rule requires no code deployment — just a table update.

On top of that, an **AI layer** automatically suggests new rules by studying the data and learning from past failures — reducing the time to author quality rules for a new dataset from hours to minutes.

---

## 2. The Problem It Solves

| Before | After (This POC) |
|---|---|
| Rules hardcoded in pipeline scripts | Rules stored in a central Delta table, shared across all pipelines |
| Changing a rule = code change + deployment | Changing a rule = one table update, live on next run |
| No visibility into which rules fire or how often | Per-rule violation tracking with trending dashboard |
| Rules written manually by engineers | AI profiles the data and suggests rules automatically |
| Bad data found late (or not at all) | Bad records quarantined instantly with full error context |
| No learning from past failures | Quarantine patterns feed back into new rule suggestions |

---

## 3. System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│                                                                     │
│   📄 CSV / ADLS                    📡 Azure Event Hubs (Kafka)      │
│   (Batch sales data)               (Real-time POS events)          │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌──────────────────────┐        ┌─────────────────────────┐
│   BATCH PIPELINE     │        │   STREAMING PIPELINE    │
│                      │        │                         │
│  Load → Validate →   │        │  Consume → Validate →   │
│  Clean / Quarantine  │        │  Clean / Quarantine     │
└──────────┬───────────┘        └────────────┬────────────┘
           │                                 │
           │      Both pipelines read from   │
           │      ╔═══════════════════════╗  │
           └─────►║  dq_rules_registry   ║◄─┘
                  ║  (Central Rule Table) ║
                  ╚═══════════════════════╝
                             ▲
                             │  Approved rules promoted here
                  ╔══════════╧══════════╗
                  ║  dq_rules_staging   ║
                  ║  (Review Queue)     ║
                  ╚══════════╤══════════╝
                             │  AI suggests rules here
               ┌─────────────┴──────────────┐
               │                            │
    ┌──────────▼──────────┐    ┌────────────▼────────────┐
    │   AI RULE SUGGESTER │    │   QUARANTINE ANALYZER   │
    │                     │    │                         │
    │  Profile data →     │    │  Study past failures →  │
    │  Suggest rules via  │    │  Suggest new rules via  │
    │  statistics + LLM   │    │  LLM                    │
    └─────────────────────┘    └─────────────────────────┘

                  ╔═══════════════════════╗
                  ║   dq_rule_metrics     ║
                  ║  (Violation Tracking) ║
                  ╚═══════════╤═══════════╝
                              │
                  ┌───────────▼───────────┐
                  │  LAKEVIEW DASHBOARD   │
                  │  Rule Effectiveness   │
                  └───────────────────────┘
```

---

## 4. What Has Been Delivered

| Component | What It Does | Status |
|---|---|---|
| **Batch Pipeline** | Loads CSV sales data, validates against rules, splits into clean and quarantine Delta tables | ✅ Complete |
| **Streaming Pipeline** | Consumes live POS events from Azure Event Hubs, validates in real time, streams clean/quarantine to Delta | ✅ Complete |
| **Central Rule Registry** | Single Delta table (`dq_rules_registry`) that governs all rules for all pipelines | ✅ Complete |
| **AI Rule Suggester** | Profiles any table, generates statistical rules automatically, calls LLM (OpenRouter/Claude/Azure OpenAI) for business-logic rules | ✅ Complete |
| **Human Review Queue** | All AI-suggested rules land in `dq_rules_staging` with `pending` status — nothing goes live without approval | ✅ Complete |
| **Quarantine Analyzer** | Mines quarantine tables for recurring failure patterns, feeds them to LLM to suggest gap-closing rules | ✅ Complete |
| **Rule Effectiveness Tracking** | Records per-rule violation counts after every run into `dq_rule_metrics` | ✅ Complete |
| **Lakeview Dashboard** | Four-panel visual: effectiveness summary, violation trend, top offenders, zero-fire candidates | ✅ Complete |

---

## 5. How Each Component Works

### 5.1 Batch Pipeline

```
CSV file (ADLS)
      │
      ▼
  Load into Delta (sales_staging)
      │
      ▼
  Apply DQX Rules  ◄──── dq_rules_registry (6 rules)
      │
      ├──► ✅ No errors?  ──► sales_clean
      │
      └──► ❌ Has errors? ──► sales_quarantine
                                    │
                                    └──► dq_rule_metrics (which rule fired, how many times)
```

**Rules currently enforced (batch):**

| Rule | What It Checks |
|---|---|
| `event_id_not_null` | Every transaction must have an ID |
| `quantity_positive` | Quantity must be greater than zero |
| `price_positive` | Unit price must be greater than zero |
| `division_valid` | Division code must be 011, 012, or 013 |
| `customer_id_not_null` | Customer ID must be present |
| `transaction_date_not_future` | Transaction date cannot be in the future |

---

### 5.2 Streaming Pipeline

```
Azure Event Hubs (Kafka endpoint)
      │
      ▼
  Spark Structured Streaming
      │
      ▼
  Apply DQX Rules  ◄──── dq_rules_registry (6 rules, loaded once at startup)
      │
      ├──► ✅ Clean events  ──► stream_events_clean   (Delta, append)
      │
      └──► ❌ Bad events    ──► stream_events_quarantine (Delta, append)
                                    │
                                    └──► dq_rule_metrics (per micro-batch)
                                    └──► stream_batch_metrics (aggregate counts)
```

> **Key design note:** Rules are loaded once when the consumer starts. To pick up a newly promoted rule, the consumer must be restarted.

---

### 5.3 AI Rule Suggester

```
Any Delta Table (e.g. sales_staging)
      │
      ▼
  PROFILER
  Scans every column:
  · What % of values are missing?
  · How many unique values exist?
  · What are the min / max / average?
  · What do sample values look like?
      │
      ▼
  STATISTICAL ENGINE                    LLM (OpenRouter / Claude / Azure OpenAI)
  Auto-generates rules from patterns:   Reads the profile + context description.
  · Always non-null → suggest null check Suggests business logic rules that
  · Always positive → suggest min check  statistics alone can't detect:
  · Small value set → suggest allowlist  · Cross-column relationships
                                         · Domain-specific anomalies
      │                                        │
      └────────────────┬────────────────────────┘
                       ▼
              dq_rules_staging
              (status = pending)
                       │
              Human reviews each rule
                       │
              ├──► ✅ Approve ──► dq_rules_registry (live immediately)
              └──► ❌ Reject  ──► stays in staging, never enforced
```

---

### 5.4 Quarantine Analyzer

```
sales_quarantine / stream_events_quarantine
      │
      ▼
  PATTERN EXTRACTOR
  · Which rules fired most?
  · What did the bad data look like?
  · What % of quarantine does each rule account for?
      │
      ▼
  LLM
  "Given these recurring failure patterns,
   suggest new rules to close the gaps"
      │
      ▼
  dq_rules_staging (status = pending)
      │
  Same human review → approve → live
```

---

### 5.5 Rule Lifecycle

```
          AI Suggester                    Quarantine Analyzer
          or Manual Entry
               │                                │
               ▼                                ▼
        ┌─────────────┐               ┌─────────────────┐
        │   PENDING   │               │     PENDING     │
        │ (staging)   │               │   (staging)     │
        └──────┬──────┘               └────────┬────────┘
               │                               │
        Human reviews                   Human reviews
               │                               │
       ┌───────┴────────┐             ┌────────┴────────┐
       ▼                ▼             ▼                  ▼
  ┌─────────┐     ┌──────────┐  ┌─────────┐      ┌──────────┐
  │APPROVED │     │REJECTED  │  │APPROVED │      │REJECTED  │
  │(staging)│     │(staging) │  │(staging)│      │(staging) │
  └────┬────┘     └──────────┘  └────┬────┘      └──────────┘
       │                             │
       └──────────────┬──────────────┘
                      ▼
             dq_rules_registry
             (LIVE — enforced on
              next pipeline run)
```

---

### 5.6 Rule Effectiveness Dashboard

Four panels, all driven from `dq_rule_metrics`, filterable by rule group:

| Panel | What It Shows | Chart Type |
|---|---|---|
| **Effectiveness Summary** | Per rule: total violations, avg violation %, last run count | Table |
| **Violation Trend** | How each rule's violation count changes run over run | Line chart |
| **Top Offenders** | The 10 rules catching the most bad records | Bar chart |
| **Zero-fire Candidates** | Rules with no violations in last N days — potential review/retirement | Table |

---

## 6. Delta Tables Reference

| Table | Purpose | Written By | Read By |
|---|---|---|---|
| `dq_rules_registry` | Live, enforced rules | `promote_rule()` | Batch + Streaming pipelines |
| `dq_rules_staging` | AI-suggested rules awaiting review | AI Suggester, Quarantine Analyzer | Human reviewer |
| `sales_staging` | Raw batch sales data | Batch setup task | Batch validation |
| `sales_clean` | Batch records that passed all rules | Batch pipeline | Downstream consumers |
| `sales_quarantine` | Batch records that failed one or more rules | Batch pipeline | Quarantine Analyzer |
| `stream_events_clean` | Streaming records that passed all rules | Streaming pipeline | Downstream consumers |
| `stream_events_quarantine` | Streaming records that failed one or more rules | Streaming pipeline | Quarantine Analyzer |
| `stream_batch_metrics` | Aggregate clean/quarantine counts per micro-batch | Streaming pipeline | Monitoring |
| `dq_rule_metrics` | Per-rule violation counts per run | Batch + Streaming pipelines | Lakeview Dashboard |

---

## 7. Step-by-Step Run Guide

> **Assumption:** Databricks cluster is running with all environment variables set, Repos synced to latest branch, and DQX library installed.

---

### Phase 1 — Batch Pipeline (one job, six chained tasks)

**Goal:** Load sales data, validate it, auto-generate rule suggestions, report, and alert — end to end from a single trigger.

As of the job-orchestration pass, `[RTM] Batch DQX Pipeline` runs everything that used to be five separate manually-triggered jobs, as one DAG:

```
stage_data ──► validate_data ──► suggest_rules ──────► run_report ──► check_alerts
                                └► analyze_quarantine ─┘
```

```
Step 1 ──► Run: [RTM] Batch DQX Pipeline   (single job trigger)

           stage_data          Load CSV into sales_staging
           validate_data       Apply DQX rules → sales_clean / sales_quarantine
                                + write dq_rule_metrics
           suggest_rules       AI Rule Suggester profiles sales_staging,
                                writes suggestions to dq_rules_staging
           analyze_quarantine  Mines this run's sales_quarantine for patterns,
                                writes suggestions to dq_rules_staging
           run_report          Rule Effectiveness Report — reads dq_rule_metrics
           check_alerts        Compares this run's violation_pct to baseline,
                                notifies Slack on a spike, logs to dq_alert_log

           Expected output:
           · sales_staging populated (e.g. 10,000 rows)
           · sales_clean: ~85% of rows
           · sales_quarantine: ~15% of rows (bad data injected intentionally)
           · dq_rule_metrics: 6 rows (one per rule that fired)
           · dq_rules_staging: new AI + quarantine-derived suggestions, status='pending'
           · dq_alert_log: any spikes detected this run

Step 2 ──► Query sales_quarantine to see which rules fired and why
           (Each quarantine record has an _errors column listing the rule name
            and the reason it failed)

Step 3 ──► Review pending rules in dq_rules_staging:
           SELECT * FROM dq_rules_staging WHERE status = 'pending'

           For each rule, decide:
           · Approve → call promote_rule() → rule goes live in dq_rules_registry
           · Reject  → call reject_rule()  → rule stays in staging, never enforced

Step 4 ──► Re-run [RTM] Batch DQX Pipeline to confirm any promoted rule fires
```

Any individual task (`stage_data`, `suggest_rules`, `run_report`, etc.) can still be re-run on its own from the Databricks Jobs UI ("Repair run" / "Run task") without re-running the whole pipeline — the DAG only changes the default, not what's possible ad hoc.

**Note:** this pipeline is manually triggered by design — the POC runs against a static sample CSV with no live-data cadence, so no schedule is attached. Wire a `schedule:` block on `rtm_batch_pipeline` in `databricks.yml` once there's an actual recurring data drop to trigger it.

---

### Phase 2 — Streaming Pipeline (one job, two concurrent tasks)

**Goal:** Simulate live POS traffic, validate events in real time — one trigger starts both producer and consumer.

`[RTM] Kafka Streaming Pipeline` combines the old `rtm_kafka_producer` and `rtm_kafka_consumer` jobs into one job with two tasks that start concurrently. The consumer can't be expressed as a `depends_on` predecessor (it runs forever, so it never reaches the terminal state `depends_on` waits for) — instead, the consumer writes a readiness marker to `stream_consumer_ready` right after the stream starts listening, and the producer polls that table (up to 2 minutes) before it starts sending events.

```
Step 1 ──► Run: [RTM] Kafka Streaming Pipeline   (single job trigger)

           stream_and_validate   Starts the Spark stream, writes a readiness
                                  marker to stream_consumer_ready, then blocks
                                  on awaitTermination() — runs indefinitely
           produce_events         Polls stream_consumer_ready (up to 120s),
                                  then sends 1,000 events (10 every 5 seconds)

Step 2 ──► Watch micro-batch logs every 30 seconds:
           "Batch 0: 42 clean | 8 quarantined"
           "Batch 1: 47 clean | 13 quarantined"

           Expected output:
           · stream_events_clean: growing with each batch
           · stream_events_quarantine: ~15% of events
           · stream_batch_metrics: one row per micro-batch
           · dq_rule_metrics: per-rule breakdown per batch
           · [RTM] DQ Alert Check (Streaming) fires every 15 minutes against
             this data (streaming has no "run completed" moment to hang an
             inline alert check off of, so it stays on a schedule)

Step 3 ──► Stop the job when done:
           Workflows → [RTM] Kafka Streaming Pipeline → Cancel Run
           (produce_events finishes on its own once it hits num_events;
            stream_and_validate keeps running until cancelled)
```

---

### Phase 3 — Rule Effectiveness & Alerting

**Goal:** Understand which rules are working, and get notified when one spikes.

`run_report` and `check_alerts` now run automatically at the end of every `[RTM] Batch DQX Pipeline` run (Phase 1, Step 1) and on a 15-minute schedule for streaming (`[RTM] DQ Alert Check (Streaming)`). To review results:

```
Step 1 ──► Open the "DQX Rule Effectiveness" Lakeview Dashboard, or query
           dq_rule_metrics directly

Step 2 ──► Set filter: run_config = sales_batch_rules (or streaming_event_rules)

Step 3 ──► Read the four panels:

           Effectiveness Summary ──► Which rules fired, how often, trend direction
           Violation Trend       ──► Are violations increasing or decreasing over time?
           Top Offenders         ──► Which rules catch the most bad data?
           Zero-fire Candidates  ──► Which rules haven't fired recently? (consider reviewing)

Step 4 ──► Use zero-fire list to decide:
           · Is the rule too permissive? Tighten it.
           · Has the data quality genuinely improved? Keep it but monitor.
           · Is the rule irrelevant? Reject/retire it.

Step 5 ──► Check dq_alert_log for any spikes and confirm the Slack notification
           landed in the configured channel.
```

---

## 8. End-to-End Flow Summary

```
Raw Data (CSV / Event Hubs)
        │
        ▼
┌────────────────────────┐
│  Pipeline runs         │  [RTM] Batch DQX Pipeline (Phase 1)
│  stage_data ──►        │  or [RTM] Kafka Streaming Pipeline (Phase 2)
│  validate_data         │  — one trigger, fully chained
└────────┬───────────────┘
         │
         ├──► ✅ Clean records ──────────────────────────► Downstream use
         │
         └──► ❌ Quarantine records
                    │
                    ├──► dq_rule_metrics ──► run_report (Phase 3 Dashboard)
                    │                              │
                    │                              ▼
                    │                        check_alerts ──► Slack (on spike)
                    │
                    └──► analyze_quarantine (auto-chained after validate_data)
                                │
                                └──► New rule suggestions
                                            │
                          ┌─────────────────┘
                          │  Also: suggest_rules (auto-chained after
                          │  validate_data) profiles source data
                          ▼
                   dq_rules_staging
                   (pending review)
                          │
                   Human approves
                          │
                          ▼
                   dq_rules_registry
                   (live on next run)
```

Everything below `validate_data` in the batch path — `suggest_rules`, `analyze_quarantine`, `run_report`, `check_alerts` — now runs automatically as part of the same job (see Phase 1). Nothing auto-promotes to `dq_rules_registry`; a human still reviews and approves every suggestion.

---

## 9. Key Metrics

| Metric | Value |
|---|---|
| Pipelines running on Databricks | Batch ✅ Streaming ✅ |
| DQX rules in production | 12 (6 batch + 6 streaming) |
| AI Rule Suggester — live and tested | ✅ |
| Quarantine Analyzer — live and tested | ✅ |
| Rule Effectiveness Dashboard | ✅ |
| Secrets in plaintext / code | 0 (all in Azure Key Vault) |
| Unit tests passing | 30+ (no Databricks connection needed) |

---

## 10. What's Next — Roadmap

| Stage | What It Adds | Business Value | Status |
|---|---|---|---|
| **A — Alerting** | Notify via Slack when a rule's `violation_pct` spikes above its own historical baseline | Operational — catch data incidents before they reach downstream consumers | ✅ Done |
| **B — Drift Detection** | Scheduled re-profiling detects when data distributions shift; auto-triggers AI Rule Suggester for affected columns | Strategic — rules stay current as data evolves, no manual monitoring needed | Not started |
| **C — DQ Scorecard** | Single 0–100 health score per table, trending over time, visible to leadership | Reporting — one number summarises data health for executive dashboards | Not started |
| **D — Rule Governance** | Rule versioning, audit trail (who approved what and when), soft-delete, expiry dates | Compliance — production-grade change management for governed environments | Not started |
| **E — Multi-table Expansion** | Apply the full framework to additional source tables / domains | Scale — proves the framework generalises beyond the initial retail POS dataset | Not started |

### Stage A — Alerting: Implementation Notes

Two complementary delivery paths were built:

**1. In-repo job (primary path)**
- `ai_suggester/alerting.py` — `detect_spikes()` compares each rule's most recent `violation_pct` (from `dq_rule_metrics`) against the average of its prior `ALERT_LOOKBACK_RUNS` runs. A rule "spikes" when `last_run_pct > max(baseline_pct * ALERT_SPIKE_MULTIPLIER, ALERT_MIN_ABSOLUTE_PCT)` (constants in `config/constants.py`). Spikes are posted to Slack via Incoming Webhook and logged to `dq_alert_log` (audit trail + dedupe so the same run never re-alerts).
- `notebooks/dq_alert_check.py` reads the webhook URL from a Databricks secret scope (never a plaintext parameter) and runs the check.
- For the **batch** path, `check_alerts` runs as the last task of `[RTM] Batch DQX Pipeline` (see section 7, Phase 1) — triggered by the pipeline completing, not a clock. For the **streaming** path, which has no "run completed" moment to hang a trigger off of, `[RTM] DQ Alert Check (Streaming)` stays on a 15-minute cron.

**2. Native Databricks SQL Alert (alternative path)**
For teams that prefer zero-code, UI-configured alerting:
1. Create a Databricks SQL query against `dq_rule_metrics` using the same baseline-vs-last-run logic as `detect_spikes()` (see the CTE in `ai_suggester/alerting.py`).
2. Create a SQL Alert on that query with a "row count > 0" trigger condition.
3. Attach a notification destination (Slack, Teams, email, PagerDuty, webhook) via the Databricks UI under **Workspace Settings → Notification Destinations**.

This path requires no deployment but lives outside version control — the in-repo job remains the source of truth for the alerting logic itself.
