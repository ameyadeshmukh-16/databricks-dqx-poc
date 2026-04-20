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
1. Clone repo
2. Create virtual environment
3. Install dependencies:
   pip install -r requirements.txt
4. Configure .env file
5. Run:
   python -m notebooks.stream_simulator
   python -m notebooks.streaming_dqx_validation

## Demo Flow
1. Start simulator → generates bad data
2. Stream ingests data
3. DQX applies rules
4. Output:
   - Clean data → curated table
   - Bad data → quarantine table
  
## Key Features
- Rule-driven validation (YAML-based)
- Supports both batch & streaming
- Scalable on Databricks Lakehouse
- Detailed row-level error tracking

## Future Enhancements
- Integrate DQX dashboard
- Auto rule generation (profiling)
- Data contract enforcement
- CI/CD integration
