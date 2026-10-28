"""Guard the Kafka consumer options that make the stream restartable after a gap."""

import importlib

import pytest


@pytest.fixture
def kafka_options(monkeypatch):
    """KAFKA_SPARK_OPTIONS built from a known environment (the module reads env vars at import)."""
    for key, value in {
        "EVENTHUB_CONN_STR": "Endpoint=sb://x/;SharedAccessKeyName=k;SharedAccessKey=v",
        "KAFKA_BOOTSTRAP_SERVERS": "x.servicebus.windows.net:9093",
        "KAFKA_TOPIC": "t",
        "KAFKA_CONSUMER_GROUP": "g",
        "AZURE_STORAGE_KEY": "k",
    }.items():
        monkeypatch.setenv(key, value)
    import rtm_kafka.config.kafka_config as kafka_config
    return importlib.reload(kafka_config).KAFKA_SPARK_OPTIONS


def test_stale_checkpoints_do_not_kill_the_stream(kafka_options):
    assert kafka_options["failOnDataLoss"] == "false"


def test_the_shaded_jaas_module_is_still_used(kafka_options):
    assert "kafkashaded." in kafka_options["kafka.sasl.jaas.config"]
