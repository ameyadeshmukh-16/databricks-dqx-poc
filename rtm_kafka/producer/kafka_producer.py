"""
Kafka Producer — Azure Event Hubs Kafka Protocol

Simulates a continuous stream of retail POS sales events and sends them to
Azure Event Hubs using the Kafka protocol endpoint (SASL_SSL, port 9093).

Uses confluent-kafka (the officially recommended Python client for Azure Event Hubs).
kafka-python is NOT used here — it has a known incompatibility with Azure EH where
API version auto-detection causes a 60-second metadata timeout on the first send.

Bad-data injection rates are defined in config/constants.py and exercised identically
by the Databricks Job entry point (notebooks/kafka_producer_job.py).

Run this script locally while kafka_stream_dqx_validation.py is active on the cluster:
    python -m rtm_kafka.producer.kafka_producer
"""

# -----------------------------------
# Imports
# -----------------------------------
import json
import time
from dotenv import load_dotenv
from confluent_kafka import Producer

from rtm_kafka.config.kafka_config import KAFKA_PRODUCER_CONFIG, TOPIC
from rtm_kafka.producer.event_generator import generate_event
from src.logger import get_logger
from config.constants import PRODUCER_BATCH_SIZE, PRODUCER_INTERVAL_SECS

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

logger = get_logger(__name__)

# -----------------------------------
# Delivery Callback
# -----------------------------------
# confluent-kafka is asynchronous by default — delivery_callback is invoked
# once per message after the broker acknowledges (or rejects) it.
def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)

# -----------------------------------
# Kafka Producer
# -----------------------------------
# Producer() accepts the config dict directly.
# Unlike kafka-python, instantiation triggers the actual connection attempt,
# so a failure here means auth or network issues (not a lazy-connect false positive).
logger.info("Initialising Kafka producer (Azure Event Hubs)")

try:
    producer = Producer(KAFKA_PRODUCER_CONFIG)
except Exception as exc:
    logger.error("Failed to initialise producer: %s", exc)
    raise

logger.info("Target   : %s", KAFKA_PRODUCER_CONFIG["bootstrap.servers"])
logger.info("Topic    : %s", TOPIC)

# -----------------------------------
# Simulation Loop
# -----------------------------------
# Sends batches of PRODUCER_BATCH_SIZE events every PRODUCER_INTERVAL_SECS seconds.
# producer.poll(0) after each produce() drains the delivery callback queue without blocking.
# producer.flush() after each batch ensures all messages are acknowledged before sleeping.
logger.info(
    "Kafka simulator started. Sending batches of %d every %ss. Press Ctrl+C to stop.",
    PRODUCER_BATCH_SIZE,
    PRODUCER_INTERVAL_SECS,
)

i = 0
try:
    while True:
        for _ in range(PRODUCER_BATCH_SIZE):
            producer.produce(
                TOPIC,
                value=json.dumps(generate_event(i)).encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)    # non-blocking callback drain
            i += 1

        producer.flush()        # wait for all in-flight messages to be acknowledged
        logger.info("Sent batch — total events dispatched: %d", i)
        time.sleep(PRODUCER_INTERVAL_SECS)

except KeyboardInterrupt:
    logger.info("Simulator stopped by user.")
    producer.flush()
    logger.info("Producer closed cleanly.")
except Exception as exc:
    logger.error("Unexpected error in producer loop: %s", exc)
    producer.flush()
    raise
