# Databricks DQX POC – Real-Time & Batch Data Quality Framework
A production-style Data Quality framework using Databricks DQX supporting:
- Batch validation
- Real-time streaming validation
- Quarantine pattern (Dead-letter architecture)

## Problem Statement
Retail organizations struggle with:
- Invalid transactions entering downstream systems
- No visibility into data quality issues
- No real-time validation in streaming pipelines

## Solution
This POC demonstrates:
- In-transit validation using DQX
- Real-time streaming validation using Event Hub
- Automatic quarantine of bad records
- Rule-driven validation using YAML configs

## Setup
1. Clone the repo and create a virtual environment.
2. Install dependencies for local development and tests:
   `pip install -r requirements.txt`
3. Install the **current** Databricks CLI (`winget install Databricks.DatabricksCLI`). The legacy
   `pip` CLI (0.18) has no `bundle` command. Authenticate to the workspace:
   `databricks auth login --host <workspace-url> --profile <profile>`
4. Set the target in `config/constants.py` (`CATALOG`, `DATABASE`) and in `databricks.yml`
   (workspace `host`, `cluster_id`). Do not leave `DATABRICKS_CLUSTER_ID` set in your environment -
   the CLI treats it as a cluster override.
5. Cluster environment variables (Compute → Advanced → Environment variables) map the
   Key Vault-backed secret scope `dqx-poc` into `AZURE_STORAGE_KEY`, `EVENTHUB_CONN_STR`,
   `OPENROUTER_API_KEY`, plus the Kafka settings. See *Environment Variables* in the technical doc.

Python dependencies on the cluster are declared in `databricks.yml` (`pipeline_libraries`) and
installed automatically for every task.

## Deploy and Run
```
databricks bundle validate -t dev -p <profile>
databricks bundle deploy   -t dev -p <profile>

# Fresh catalog only: load the DQX rules into dq_rules_registry
databricks bundle run rtm_seed_rules -t dev -p <profile>

# Batch: stage -> validate -> suggest/analyze -> report -> alerts
databricks bundle run rtm_batch_pipeline -t dev -p <profile>

# Streaming: consumer + producer; the consumer runs until you cancel the run
databricks bundle run rtm_kafka_streaming -t dev -p <profile>
```

## Demo Flow
1. Producer generates events with injected bad data (`rtm_kafka_streaming`) and the batch job
   stages the sales CSV
2. DQX applies the rules from `dq_rules_registry`
3. Output:
   - Clean data → `*_clean` tables
   - Bad data → `*_quarantine` tables
   - Per-rule violation metrics → `dq_rule_metrics`
4. AI suggester and quarantine analyzer propose new rules into `dq_rules_staging` for review;
   spike detection writes to `dq_alert_log`

## Documentation
- `DQX_POC_Technical_Documentation.md` - architecture, every module, table schemas, jobs and a
  troubleshooting table of deployment errors (§17)
- `DQX_POC_Documentation.md` - management-facing overview
  
## Key Features
- Rule-driven validation (rules stored in a Delta registry, seeded from YAML)
- Supports both batch & streaming
- Scalable on Databricks Lakehouse
- Detailed row-level error tracking

## Future Enhancements
- Integrate DQX dashboard
- Auto rule generation (profiling)
- Data contract enforcement
- CI/CD integration
