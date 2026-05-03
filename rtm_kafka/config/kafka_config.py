"""
Kafka Configuration — Azure Event Hubs Kafka Protocol Endpoint

This module is the single source of truth for all Kafka connection parameters.
It exposes two config objects:

  - KAFKA_PRODUCER_CONFIG  : passed directly to kafka-python KafkaProducer(...)
  - KAFKA_SPARK_OPTIONS    : passed to Spark .format("kafka").options(**...)

Azure Event Hubs supports the Kafka protocol natively on port 9093.
Authentication uses SASL_SSL with PLAIN mechanism — the existing EVENTHUB_CONN_STR
doubles as the SASL password, so no new secrets are required.
"""

# -----------------------------------
# Imports
# -----------------------------------
import json
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
# Producer Config (kafka-python)
# -----------------------------------
# Used by KafkaProducer(**KAFKA_PRODUCER_CONFIG) in kafka/producer/kafka_producer.py
KAFKA_PRODUCER_CONFIG = {
    "bootstrap_servers":  BOOTSTRAP_SERVERS,
    "security_protocol":  "SASL_SSL",
    "sasl_mechanism":     "PLAIN",
    "sasl_plain_username": "$ConnectionString",            # Azure EH requires this literal string
    "sasl_plain_password": EVENTHUB_CONN_STR,              # Full connection string is the password
    "value_serializer":   lambda v: json.dumps(v).encode("utf-8"),
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
