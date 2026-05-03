"""
Kafka Configuration — Azure Event Hubs Kafka Protocol Endpoint

This module is the single source of truth for all Kafka connection parameters.
It exposes two config objects:

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
import os
from dotenv import load_dotenv

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Core connection values
# -----------------------------------
# Azure Event Hubs Kafka endpoint (port 9093 — NOT the SDK default of 5671)
BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

# Event Hub name doubles as the Kafka topic name
TOPIC = os.environ["KAFKA_TOPIC"]

# Consumer group for Spark Structured Streaming
CONSUMER_GROUP = os.environ["KAFKA_CONSUMER_GROUP"]

# Reuse existing Event Hub connection string as the SASL password
# Azure EH Kafka auth: username is always the literal "$ConnectionString"
EVENTHUB_CONN_STR = os.environ["EVENTHUB_CONN_STR"]

# -----------------------------------
# Producer Config (confluent-kafka)
# -----------------------------------
# Used by Producer(KAFKA_PRODUCER_CONFIG) in rtm_kafka/producer/kafka_producer.py
# confluent-kafka uses dot-notation keys (Java properties style), unlike kafka-python
# which used underscores. Serialization is NOT specified here — confluent-kafka
# takes raw bytes directly at produce() time, not via a config-level serializer.
KAFKA_PRODUCER_CONFIG = {
    "bootstrap.servers":  BOOTSTRAP_SERVERS,
    "security.protocol":  "SASL_SSL",
    "sasl.mechanism":     "PLAIN",
    "sasl.username":      "$ConnectionString",             # Azure EH requires this literal string
    "sasl.password":      EVENTHUB_CONN_STR,               # Full connection string is the password
}

# -----------------------------------
# Spark Consumer Options (.format("kafka"))
# -----------------------------------
# JAAS config is the Spark/JVM equivalent of the SASL credentials above.
# The format must match exactly what the Kafka client library expects.
_JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{EVENTHUB_CONN_STR}";'
)

# Used by spark.readStream.format("kafka").options(**KAFKA_SPARK_OPTIONS)
KAFKA_SPARK_OPTIONS = {
    "kafka.bootstrap.servers":  BOOTSTRAP_SERVERS,
    "subscribe":                TOPIC,
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.sasl.jaas.config":   _JAAS_CONFIG,
    "kafka.group.id":           CONSUMER_GROUP,
    "startingOffsets":          "latest",           # Pick up live events only; use "earliest" for replay
}
