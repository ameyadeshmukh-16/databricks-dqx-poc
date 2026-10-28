"""
Kafka Configuration — Azure Event Hubs Kafka Protocol Endpoint

Single source of truth for all Kafka connection parameters.
Exposes two config objects:

  - KAFKA_PRODUCER_CONFIG  : passed directly to confluent_kafka.Producer(...)
  - KAFKA_SPARK_OPTIONS    : passed to Spark .format("kafka").options(**...)

Azure Event Hubs supports the Kafka protocol natively on port 9093.
Authentication uses SASL_SSL with PLAIN mechanism — the existing EVENTHUB_CONN_STR
doubles as the SASL password, so no new secrets are required.

Note on client libraries:
  - Producer uses confluent-kafka (dot-notation config keys, e.g. "bootstrap.servers")
  - Spark consumer uses its built-in JVM Kafka connector — entirely independent
  - kafka-python is NOT used; it has a known incompatibility with Azure Event Hubs
    where API version auto-detection times out on first send
"""

# -----------------------------------
# Imports
# -----------------------------------
from dotenv import load_dotenv
from config.settings import Settings

# -----------------------------------
# Environment
# -----------------------------------
# load_dotenv() is a no-op on Databricks (env vars come from cluster config);
# it is needed locally to pick up values from .env
load_dotenv()

# -----------------------------------
# Settings
# -----------------------------------
_settings = Settings.from_env()

# Convenience aliases used by notebooks that import TOPIC directly
TOPIC          = _settings.kafka_topic
CONSUMER_GROUP = _settings.kafka_consumer_group

# -----------------------------------
# Producer Config (confluent-kafka)
# -----------------------------------
# Used by Producer(KAFKA_PRODUCER_CONFIG) in rtm_kafka/producer/kafka_producer.py.
# confluent-kafka uses dot-notation keys (Java properties style).
# Serialization is NOT specified here — confluent-kafka takes raw bytes at produce() time.
KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers": _settings.kafka_bootstrap_servers,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     "$ConnectionString",       # Azure EH requires this literal string
    "sasl.password":     _settings.eventhub_conn_str,
}

# -----------------------------------
# Spark Consumer Options (.format("kafka"))
# -----------------------------------
# JAAS config is the Spark/JVM equivalent of the SASL credentials above.
# Databricks Runtime ships a shaded Kafka client, so the login module needs the
# kafkashaded. prefix (the unshaded name is not found on current runtimes).
_JAAS_CONFIG = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{_settings.eventhub_conn_str}";'
)

# Used by spark.readStream.format("kafka").options(**KAFKA_SPARK_OPTIONS)
KAFKA_SPARK_OPTIONS = {
    "kafka.bootstrap.servers": _settings.kafka_bootstrap_servers,
    "subscribe":               TOPIC,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  _JAAS_CONFIG,
    "kafka.group.id":          CONSUMER_GROUP,
    "startingOffsets":         "latest",    # pick up live events only; use "earliest" for replay
    # Event Hubs only keeps data for a limited time. If the stream is restarted after a gap, its checkpoint
    # can point at offsets that no longer exist; by default Spark then refuses to continue and the stream
    # dies on its first batch. Skipping the expired range is the right behaviour for this pipeline.
    "failOnDataLoss":          "false",
}
