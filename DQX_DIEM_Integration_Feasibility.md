# DQX Data Quality Framework — DIEM Integration Feasibility Brief

> **Purpose:** Describe the current, working state of a Databricks-based data quality proof-of-concept (built on Databricks Labs DQX) so it can be evaluated for potential inclusion in the DIEM (Data Ingestion and Engineering Management) framework.
> **Audience:** DIEM framework owners / architects assessing integration fit.
> **Scope:** Built and functioning capabilities only. This is not a roadmap document.

---

## 1. Executive Summary

This is a working proof-of-concept that uses **Databricks Labs DQX** to centralize data quality rule enforcement for both batch and streaming pipelines. Instead of hardcoding validation logic inside each pipeline, all rules live in a governed Delta table that every pipeline reads at runtime — so a rule change is a table update, not a code deployment. On top of that base pattern, the POC adds an AI layer that profiles data and proposes new rules (with mandatory human review before anything goes live), a quarantine-mining loop that learns from past failures, a per-rule effectiveness dashboard, and a spike-detection alerting path.

The core pattern — **externalized, table-driven rule governance with a human-reviewed promotion workflow** — is the part most likely to generalize into a shared framework like DIEM. The current implementation, however, is a single-team POC: it has hardcoded Unity Catalog names, a fixed cluster ID, Azure Event Hubs-specific streaming config, and direct-to-SaaS LLM calls. None of that is architecturally required by the pattern itself, but all of it would need to be abstracted or reconciled with DIEM's existing conventions before this could be adopted as shared infrastructure rather than copied as a one-off. Section 5 itemizes exactly what would need to change.

---

## 2. What This Solves

| Before | This Approach |
|---|---|
| Rules hardcoded inside individual pipeline scripts | Rules stored in one central Delta table (`dq_rules_registry`), read by every pipeline at runtime |
| Changing a rule requires a code change + deployment | Changing a rule is a table update — live on the next pipeline run, no redeploy |
| No visibility into which rules fire, or how often | Per-rule violation counts tracked every run, visualized in a dashboard |
| Rules authored manually, one at a time, by engineers | Data is auto-profiled and rule candidates are suggested (statistically and via LLM); a human still approves everything before it's enforced |
| Bad records discovered late, or not at all | Failing records are split into a quarantine table immediately, with structured error detail attached per record |
| No feedback loop from production failures | Quarantine patterns are mined and fed back into new rule suggestions |

The underlying enforcement engine is [Databricks Labs DQX](https://github.com/databrickslabs/dqx) — an open-source Databricks Labs project for declarative data quality checks on Spark DataFrames. This POC's contribution on top of DQX is the rule-registry pattern, the AI-assisted authoring loop, the quarantine feedback loop, the effectiveness dashboard, and the alerting layer — none of which ship with DQX itself.

---

## 3. Architecture & Components

### 3.1 Rule Registry & Lifecycle

The center of the design is a rule lifecycle with a mandatory human checkpoint:

```
Rule candidate created (AI Suggester, Quarantine Analyzer, or manual entry)
        │
        ▼
  dq_rules_staging   (status = "pending")
        │
   Human reviews
        │
   ┌────┴────┐
   ▼         ▼
APPROVED   REJECTED
   │         (stays in staging, never enforced)
   ▼
dq_rules_registry   (live — enforced on next pipeline run / consumer restart)
```

- `dq_rules_registry` uses DQX's native nested check schema (`name`, `criticality`, `check.function`, `check.for_each_column`, `check.arguments`, `run_config_name`, etc.) — pipelines load from it via `DQEngine.load_checks(TableChecksStorageConfig(...))`.
- `dq_rules_staging` holds candidate rules with full provenance: `source` (`statistical` | `llm` | `quarantine_analysis`), `llm_backend`, `confidence`, `reasoning`, and status (`pending` | `approved` | `rejected`).
- Promotion (`promote_rule()`) and rejection (`reject_rule()`) are simple, auditable Delta operations — no rule reaches production without an explicit approval action recorded with a reviewer identity and timestamp.
- Rule groups are namespaced by `run_config_name` (e.g. `sales_batch_rules`, `streaming_event_rules`), so multiple independent rule sets can coexist in the same registry table.

### 3.2 Batch Pipeline

Loads a source dataset into a staging Delta table, applies the current rule set, and splits output into clean and quarantine tables:

```
Source data → staging table → DQX validation → clean table
                                             └─→ quarantine table → per-rule metrics
```

Quarantined records retain the original row plus a structured `_errors` array (rule name, message, function, columns involved) — nothing is dropped, and every failure is traceable to the specific rule that caught it.

### 3.3 Streaming Pipeline

Same validation logic applied to a live Kafka stream (consumed via Azure Event Hubs' Kafka-compatible endpoint in this POC):

```
Kafka topic → Spark Structured Streaming → DQX validation (per micro-batch)
                                          → clean table (append)
                                          └→ quarantine table (append) → per-rule metrics
```

- Rules are loaded once at consumer startup (not re-read per micro-batch) for performance; a newly promoted rule takes effect on the next consumer restart, not mid-stream. This is a known, documented tradeoff.
- The producer and consumer run as two tasks in the same Databricks Job, started concurrently. Because the consumer task never reaches a terminal state (it runs indefinitely until cancelled), Databricks' native `depends_on` — which waits for the upstream task to finish — can't express "start the producer only once the consumer is ready." The workaround is a readiness signal: the consumer writes a marker row once it's actively listening, and the producer polls for that marker (with a timeout) before sending its first event. This is a Databricks orchestration limitation, not a DQX limitation, and is worth flagging if DIEM has its own job-orchestration conventions that already solve this differently.

### 3.4 AI Rule Suggester

Given any Delta table, produces rule candidates through two independent paths that feed the same staging table:

- **Statistical engine** (no LLM call): profiles every column (null %, distinct count, min/max/mean/stddev, sample values) and applies deterministic thresholds — e.g. a column that's never null suggests a not-null rule; a column with a small, high-coverage value set suggests an allowlist rule; a numeric column that's always positive suggests a minimum-value rule.
- **LLM engine**: given the same column profile plus a short dataset-context description, an LLM is prompted to suggest business-logic rules the statistics alone can't infer (cross-column relationships, domain-specific constraints). Explicitly instructed not to duplicate the statistical rules.
- The LLM call goes through a **pluggable backend abstraction** — Claude, Azure OpenAI, or OpenRouter, selected via a factory function and environment variable, with no code change needed to switch providers.
- All output — statistical or LLM — lands in `dq_rules_staging` in the same shape, ready for human review.

### 3.5 Quarantine Analyzer

Closes the feedback loop: mines a quarantine table for the most frequent violation patterns (which rules fire most, on what values, at what rate), sends that summary to the same pluggable LLM backend, and asks it to suggest *new* rules that would catch related failure modes the current rule set is missing. Output goes through the identical staging → human review → promotion path as the AI Suggester — there is no separate, less-governed path for these suggestions.

### 3.6 Rule Effectiveness Dashboard

A four-panel view (built as a Databricks Lakeview dashboard) driven entirely from a per-rule, per-run metrics table:

| Panel | Purpose |
|---|---|
| Effectiveness Summary | Per rule: total violations, average violation %, most recent run's count |
| Violation Trend | How each rule's violation count moves over time (line chart) |
| Top Offenders | The rules catching the most bad data (bar chart) |
| Zero-fire Candidates | Rules with no violations across a recent lookback window — candidates for review or retirement |

This gives a concrete, queryable answer to "is this rule still doing anything," which is normally invisible once a rule is live.

### 3.7 Alerting

Two complementary delivery paths, both driven off the same violation-metrics table:

- **In-repo spike detection**: compares each rule's most recent violation percentage against the rolling average of its own prior runs. A rule is flagged when the latest run's violation rate exceeds its historical baseline by a configurable multiplier (with a minimum-absolute-percent floor so a near-zero baseline doesn't trivially "spike," and a minimum-run-count floor so a rule isn't judged before it has enough history). Flagged spikes are posted to a Slack channel via Incoming Webhook and logged to an audit table with a dedupe key so the same run never re-alerts.
- **Native Databricks SQL Alert (no-code alternative)**: the same underlying metrics table can be queried directly by a Databricks SQL Alert with a static threshold, using Databricks' built-in notification destinations (Slack, Teams, email, PagerDuty, webhook) — for teams that prefer a UI-configured path over an in-repo job. This path is simpler but only supports a fixed threshold, not the rolling-baseline comparison above.

### 3.8 Job Orchestration

Three Databricks Jobs currently deploy this whole system via a Databricks Asset Bundle (`databricks.yml`):

- **Batch pipeline** — a single job, six-task DAG: load → validate → {AI-suggest rules, mine quarantine} (parallel) → build effectiveness report → check for alert-worthy spikes. What began as several independently-triggered jobs is now one chained flow from a single trigger.
- **Streaming pipeline** — one job, two concurrent tasks (producer + consumer), coordinated via the readiness-signal handshake described in 3.3.
- **Streaming alert check** — a separately scheduled job (cron, every 15 minutes) checking the streaming rule set for spikes, since the streaming pipeline runs indefinitely and has no natural "run completed" moment to hang an inline alert check off of.

---

## 4. Data Contracts & Interfaces

This is the surface a wrapping framework would actually integrate against.

### Delta table schemas

**`dq_rules_registry`** — the live, enforced rule set (DQX's native nested schema):
```
name                  STRING
criticality           STRING           -- "error" | "warning"
check                 STRUCT<
    function              STRING,      -- "is_not_null" | "min_value" | "max_value" |
                                        -- "sql_expression" | "matches_regex" | "is_in_list"
    for_each_column       ARRAY<STRING>,
    arguments             MAP<STRING, STRING>
>
filter                STRING           -- optional row filter, NULL for all rows
run_config_name       STRING           -- namespaces independent rule groups
user_metadata         MAP<STRING, STRING>
created_at            TIMESTAMP
rule_fingerprint      STRING           -- nullable
rule_set_fingerprint  STRING           -- nullable
```

**`dq_rules_staging`** — candidate rules awaiting human review:
```
rule_id          STRING       -- UUID
name, column_name, function, constraint (JSON), criticality, message, run_config_name  STRING
source           STRING       -- "statistical" | "llm" | "quarantine_analysis"
llm_backend      STRING       -- which LLM produced it, if any
confidence       DOUBLE       -- statistical rules only
reasoning        STRING
status           STRING       -- "pending" | "approved" | "rejected"
suggested_at, reviewed_at    TIMESTAMP
reviewed_by      STRING
```

**`dq_rule_metrics`** — per-rule, per-run violation counts (feeds both the dashboard and alerting):
```
run_id, run_type ("batch"|"streaming"), run_config_name, rule_name, function   STRING
violations, total_records   LONG
violation_pct               DOUBLE
recorded_at                 TIMESTAMP
```

**`dq_alert_log`** — audit trail of every alert fired:
```
alert_id, rule_name, run_config_name, run_id   STRING
baseline_pct, last_run_pct, multiplier_used    DOUBLE
triggered_at    TIMESTAMP
notified        BOOLEAN
notify_error    STRING       -- populated only on delivery failure
```

**Clean / quarantine tables** — schema inferred from source data, plus DQX metadata columns on the quarantine side:
```
_errors:   ARRAY<STRUCT<name, message, function, columns, run_time, run_id,
                        user_metadata MAP<STRING,STRING>, rule_fingerprint,
                        rule_set_fingerprint, skipped>>
_warnings: ARRAY<STRUCT<...>>   -- same structure, non-fatal
```

### Configuration surface

All table names, thresholds, and connection parameters are centralized in two files rather than scattered across notebooks:

- A constants module holding every fully-qualified table name (derived from a single catalog + database pair), rule-group identifiers, ADLS paths, bad-data simulation rates, and AI-suggester statistical thresholds (null-percent cutoff, distinct-value cutoff, coverage threshold).
- A settings/env-var loader that validates required variables at startup (storage credentials, Event Hubs connection string, Kafka topic/consumer group) and defaults optional ones (LLM backend selection and credentials), raising a single clear error listing everything missing rather than failing deep inside a pipeline run.

This centralization is itself a candidate for reuse: **changing the catalog/database pair in one place propagates to every table reference in the system.**

---

## 5. Infra Dependencies & Portability Friction Points

These are POC-specific implementation choices, not requirements of the underlying pattern. Each would need to be reconciled with DIEM's existing conventions before this could move from "one team's proof of concept" to "shared framework capability":

| Dependency | Current state | Why it matters for DIEM fit |
|---|---|---|
| **Unity Catalog naming** | Catalog and database names are hardcoded constants (catalog name is literally derived from a specific cluster ID) | Would need to conform to whatever catalog/schema naming convention DIEM already enforces across teams |
| **Compute** | Jobs run on one `existing_cluster_id` — a pre-provisioned, always-on cluster — rather than an ephemeral job cluster spec | DIEM may standardize on job clusters (or a specific cluster policy) for cost/isolation reasons |
| **Streaming transport** | Built specifically against Azure Event Hubs' Kafka-compatible endpoint, using `confluent-kafka` (chosen over `kafka-python` because Event Hubs doesn't respond correctly to that library's connection-probe handshake) with SASL_SSL auth | If DIEM's ingestion layer already standardizes on a different message bus, or a different Event Hubs/Kafka client convention, this layer would need to be swapped, not just reconfigured |
| **Storage** | ADLS Gen2 paths and account keys are referenced directly | Would need to align with DIEM's storage-access pattern (e.g. if DIEM standardizes on Unity Catalog external locations/credentials instead of direct account keys) |
| **Alerting transport** | Slack Incoming Webhook URL, read from a specifically-named Databricks secret scope | DIEM may have its own notification-routing convention (e.g. a shared alerting service) this should plug into instead of a bespoke webhook |
| **LLM access** | Three backends (Claude, Azure OpenAI, OpenRouter) called directly with API keys from environment variables | If DIEM has (or plans) a centralized LLM gateway or governed-secrets pattern for AI features, this should route through that rather than holding keys per-project |
| **Job deployment** | Deployed via a Databricks Asset Bundle (`databricks.yml`) scoped to a single `dev` target | Would need a path to DIEM's multi-team deployment/CI convention if one exists (bundle-per-team vs. shared bundle, environment promotion strategy, etc.) |

None of these are deep architectural constraints — they're the specific choices a single-team POC makes when there's no larger framework to conform to yet. The rule-registry pattern, the staging/review/promotion lifecycle, and the metrics/alerting model do not depend on any of them.

---

## 6. Current Tech Stack Summary

| Layer | Technology |
|---|---|
| Data quality engine | Databricks Labs DQX |
| Storage | Delta Lake / Unity Catalog |
| Batch compute | Databricks notebooks via `spark_python_task` |
| Streaming compute | Spark Structured Streaming, `foreachBatch` micro-batch processing |
| Streaming transport | Azure Event Hubs (Kafka-compatible endpoint), `confluent-kafka` client |
| Job deployment | Databricks Asset Bundles (`databricks.yml`) |
| Dashboarding | Databricks Lakeview |
| AI / LLM | Pluggable backend: Anthropic Claude, Azure OpenAI, or OpenRouter |
| Alerting | Slack Incoming Webhook, or native Databricks SQL Alerts |
| Testing | pytest — the majority of logic (rule generation, response parsing, metrics math, alert-spike logic) is written to be testable without a live Spark session |

---

## 7. Suggested Questions for the DIEM Team

To move from "does this look interesting" to "is this actually adoptable," these are the questions worth putting to whoever owns DIEM's architecture:

1. Does DIEM already have a rule-governance or config-table pattern that this rule registry would need to merge into, replace, or defer to — rather than existing as a second, parallel mechanism?
2. Does DIEM standardize job orchestration in a way that already solves the "two long-running concurrent tasks" problem this POC works around with a readiness-signal handshake — and if so, should this be re-expressed in that pattern instead?
3. Is there an existing DIEM alerting/notification convention (a shared service, a standard webhook contract, a specific tool) that this should route through instead of a project-specific Slack webhook?
4. Does DIEM dictate Unity Catalog naming/structure across teams, and if so, what needs to change here to comply?
5. Is there a centralized LLM gateway, model-access policy, or secrets-governance pattern within DIEM that the AI Suggester and Quarantine Analyzer's LLM calls should be routed through, rather than holding provider API keys directly?
6. Does DIEM have its own streaming-ingestion standard (transport, client library, auth pattern) that this pipeline's Kafka/Event Hubs specifics should be re-platformed onto?
7. What is DIEM's deployment/promotion model across teams (single shared bundle vs. per-team bundles, environment promotion), and how would this project's `databricks.yml` need to restructure to fit it?
