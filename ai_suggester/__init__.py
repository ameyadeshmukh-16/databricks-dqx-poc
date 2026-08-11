"""
ai_suggester — AI-powered DQX rule suggestion engine.

Profiles any Spark DataFrame, generates statistical rules automatically,
then calls an LLM (Claude or Azure OpenAI) for business-logic rules that
statistics cannot detect. Suggestions land in dq_rules_staging for human
review before promotion to dq_rules_registry.

Also includes alerting: detects rule violation_pct spikes in dq_rule_metrics
and notifies Slack via Incoming Webhook.
"""
