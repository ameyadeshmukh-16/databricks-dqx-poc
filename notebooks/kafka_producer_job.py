"""
Kafka Producer Job (Databricks Notebook)

Runs the retail POS event producer as a Databricks Job, replacing the
local-only kafka_producer.py entry point. Parameterised via dbutils.widgets
so the job can be configured per-run from the Databricks Jobs UI or CLI.

Widget parameters:
  num_events       — total events to send; 0 = run forever until job is stopped
  batch_size       — events per batch (default: 10)
  interval_seconds — seconds to sleep between batches (default: 5)

Architecture:
  This notebook (Databricks Job)
       ↓  SASL_SSL / port 9093
  Azure Event Hubs (Kafka endpoint)
       ↓  .format("kafka")
  kafka_stream_dqx_validation.py (streaming consumer)
"""

# -----------------------------------
# Imports
# -----------------------------------
import json
from dotenv import load_dotenv

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Logging
# -----------------------------------
from src.logger import get_logger
logger = get_logger(__name__)

# -----------------------------------
# Job Widgets
# -----------------------------------
# dbutils is injected by Databricks — available in notebook and job context.
# Defaults match the local kafka_producer.py behaviour.
dbutils.widgets.text("num_events",       "500", "Total events to send (0 = run forever)")
dbutils.widgets.text("batch_size",       "10",  "Events per batch")
dbutils.widgets.text("interval_seconds", "5",   "Sleep between batches (seconds)")

num_events     = int(dbutils.widgets.get("num_events"))
batch_size     = int(dbutils.widgets.get("batch_size"))
interval_secs  = float(dbutils.widgets.get("interval_seconds"))

run_forever = (num_events == 0)

logger.info(
    "Producer job started — num_events=%s, batch_size=%d, interval=%.1fs",
    "∞" if run_forever else num_events,
    batch_size,
    interval_secs,
)

# -----------------------------------
# Kafka Producer
# -----------------------------------
from confluent_kafka import Producer
from rtm_kafka.config.kafka_config import KAFKA_PRODUCER_CONFIG, TOPIC
from rtm_kafka.producer.event_generator import generate_event

def delivery_callback(err, msg):
    if err:
        logger.error("Delivery failed: %s", err)

logger.info("Initialising producer — target: %s | topic: %s",
            KAFKA_PRODUCER_CONFIG["bootstrap.servers"], TOPIC)

try:
    producer = Producer(KAFKA_PRODUCER_CONFIG)
except Exception as exc:
    logger.error("Failed to initialise producer: %s", exc)
    raise

# -----------------------------------
# Send Loop
# -----------------------------------
import time

i = 0
try:
    while run_forever or i < num_events:
        current_batch = min(batch_size, num_events - i) if not run_forever else batch_size

        for _ in range(current_batch):
            producer.produce(
                TOPIC,
                value=json.dumps(generate_event(i)).encode("utf-8"),
                callback=delivery_callback,
            )
            producer.poll(0)
            i += 1

        producer.flush()
        logger.info("Sent batch — total events dispatched: %d", i)

        if run_forever or i < num_events:
            time.sleep(interval_secs)

except KeyboardInterrupt:
    logger.info("Job stopped by user.")
except Exception as exc:
    logger.error("Unexpected error in producer loop: %s", exc)
    raise
finally:
    producer.flush()
    logger.info("Producer closed. Total events sent: %d", i)
