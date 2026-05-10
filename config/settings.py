"""
Settings — runtime configuration loaded from environment variables.

Replaces scattered os.environ["KEY"] calls across notebooks and modules.
Call Settings.from_env() once at startup; pass the instance where needed.

Required vars raise EnvironmentError with a clear message if absent.
Optional vars (Databricks Connect, local-only) default to empty string.
"""

from dataclasses import dataclass
import os


@dataclass
class Settings:
    # Azure Storage (ADLS Gen2)
    azure_storage_key: str

    # Kafka / Azure Event Hubs
    eventhub_conn_str: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_consumer_group: str

    # Databricks Connect (local development only — not present on cluster)
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_cluster_id: str = ""

    # LLM backends — all optional; only the selected backend's keys are needed
    anthropic_api_key: str = ""
    claude_model: str = "claude-sonnet-4-6"
    azure_openai_api_key: str = ""
    azure_openai_endpoint: str = ""
    azure_openai_deployment: str = ""
    azure_openai_api_version: str = "2024-02-01"
    llm_backend: str = "claude"   # default backend: "claude" | "azure_openai"

    @classmethod
    def from_env(cls) -> "Settings":
        """Load settings from environment. Raises EnvironmentError if any required var is missing."""
        required = [
            "AZURE_STORAGE_KEY",
            "EVENTHUB_CONN_STR",
            "KAFKA_BOOTSTRAP_SERVERS",
            "KAFKA_TOPIC",
            "KAFKA_CONSUMER_GROUP",
        ]
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {missing}\n"
                "Copy .env.example to .env and fill in the values."
            )
        return cls(
            azure_storage_key       = os.environ["AZURE_STORAGE_KEY"],
            eventhub_conn_str       = os.environ["EVENTHUB_CONN_STR"],
            kafka_bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            kafka_topic             = os.environ["KAFKA_TOPIC"],
            kafka_consumer_group    = os.environ["KAFKA_CONSUMER_GROUP"],
            databricks_host         = os.environ.get("DATABRICKS_HOST", ""),
            databricks_token        = os.environ.get("DATABRICKS_TOKEN", ""),
            databricks_cluster_id   = os.environ.get("DATABRICKS_CLUSTER_ID", ""),
            anthropic_api_key       = os.environ.get("ANTHROPIC_API_KEY", ""),
            claude_model            = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-6"),
            azure_openai_api_key    = os.environ.get("AZURE_OPENAI_API_KEY", ""),
            azure_openai_endpoint   = os.environ.get("AZURE_OPENAI_ENDPOINT", ""),
            azure_openai_deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", ""),
            azure_openai_api_version = os.environ.get("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            llm_backend             = os.environ.get("LLM_BACKEND", "claude"),
        )
