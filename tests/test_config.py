"""
Tests for config/settings.py

Verifies that Settings.from_env() validates required variables correctly,
loads all values when present, and defaults optional vars to empty string.
"""

import os
import pytest
from config.settings import Settings


REQUIRED_VARS = [
    "AZURE_STORAGE_KEY",
    "EVENTHUB_CONN_STR",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_TOPIC",
    "KAFKA_CONSUMER_GROUP",
]

FULL_ENV = {
    "AZURE_STORAGE_KEY":       "test-storage-key",
    "EVENTHUB_CONN_STR":       "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=abc",
    "KAFKA_BOOTSTRAP_SERVERS": "test.servicebus.windows.net:9093",
    "KAFKA_TOPIC":             "test-topic",
    "KAFKA_CONSUMER_GROUP":    "test-consumer-group",
    "DATABRICKS_HOST":         "https://test.azuredatabricks.net/",
    "DATABRICKS_TOKEN":        "dapitest",
    "DATABRICKS_CLUSTER_ID":   "0000-000000-test",
}


def _set_env(env: dict):
    for k, v in env.items():
        os.environ[k] = v


def _clear_env():
    for k in REQUIRED_VARS + ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLUSTER_ID"]:
        os.environ.pop(k, None)


@pytest.fixture(autouse=True)
def clean_env():
    _clear_env()
    yield
    _clear_env()


def test_missing_all_required_vars_raises():
    with pytest.raises(EnvironmentError) as exc_info:
        Settings.from_env()
    assert "Missing required environment variables" in str(exc_info.value)


@pytest.mark.parametrize("missing_var", REQUIRED_VARS)
def test_missing_single_required_var_raises(missing_var):
    env = {k: v for k, v in FULL_ENV.items() if k != missing_var}
    _set_env(env)
    with pytest.raises(EnvironmentError) as exc_info:
        Settings.from_env()
    assert missing_var in str(exc_info.value)


def test_all_vars_present_loads_correctly():
    _set_env(FULL_ENV)
    settings = Settings.from_env()
    assert settings.azure_storage_key       == FULL_ENV["AZURE_STORAGE_KEY"]
    assert settings.eventhub_conn_str       == FULL_ENV["EVENTHUB_CONN_STR"]
    assert settings.kafka_bootstrap_servers == FULL_ENV["KAFKA_BOOTSTRAP_SERVERS"]
    assert settings.kafka_topic             == FULL_ENV["KAFKA_TOPIC"]
    assert settings.kafka_consumer_group    == FULL_ENV["KAFKA_CONSUMER_GROUP"]
    assert settings.databricks_host         == FULL_ENV["DATABRICKS_HOST"]
    assert settings.databricks_token        == FULL_ENV["DATABRICKS_TOKEN"]
    assert settings.databricks_cluster_id   == FULL_ENV["DATABRICKS_CLUSTER_ID"]


def test_optional_vars_default_to_empty_string():
    required_only = {k: v for k, v in FULL_ENV.items() if k in REQUIRED_VARS}
    _set_env(required_only)
    settings = Settings.from_env()
    assert settings.databricks_host       == ""
    assert settings.databricks_token      == ""
    assert settings.databricks_cluster_id == ""


def test_error_message_lists_all_missing():
    with pytest.raises(EnvironmentError) as exc_info:
        Settings.from_env()
    msg = str(exc_info.value)
    for var in REQUIRED_VARS:
        assert var in msg
