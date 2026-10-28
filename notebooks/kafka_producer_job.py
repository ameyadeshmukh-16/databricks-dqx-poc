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
import time
from datetime import datetime
from dotenv import load_dotenv

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Repo root on sys.path (bundle deploys scripts under .../files/notebooks/)
# -----------------------------------
import inspect as _inspect, os as _os, sys as _sys
_repo_root = _os.path.dirname(_os.path.dirname(_os.path.abspath(_inspect.currentframe().f_code.co_filename)))
if _repo_root not in _sys.path:
    _sys.path.insert(0, _repo_root)

# -----------------------------------
# Logging
# -----------------------------------
from src.logger import get_logger
logger = get_logger(__name__)

# -----------------------------------
# Spark Session
# -----------------------------------
# Needed only to poll the consumer's readiness signal (see below) — the
# producer itself talks to Kafka directly and has no other Spark dependency.
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# -----------------------------------
# Wait for Consumer Readiness
# -----------------------------------
# In rtm_kafka_streaming, this task starts concurrently with
# kafka_stream_dqx_validation.py rather than depends_on it — Databricks
# depends_on waits for the upstream task to terminate, and the consumer runs
# forever. Poll stream_consumer_ready instead: it's written once the consumer
# is actively listening, so events aren't sent before anything can receive them.
from config.constants import TABLE_STREAM_READY

_READY_TIMEOUT_SECS = 120
_READY_MAX_AGE_SECS = 300

_deadline = time.time() + _READY_TIMEOUT_SECS
_ready = False
while time.time() < _deadline:
    try:
        _row = spark.sql(f"SELECT MAX(started_at) AS t FROM {TABLE_STREAM_READY}").first()
        if _row and _row["t"] and (datetime.now() - _row["t"]).total_seconds() < _READY_MAX_AGE_SECS:
            _ready = True
            break
    except Exception:
        pass  # table may not exist yet if the consumer hasn't started
    time.sleep(5)

if _ready:
    logger.info("Consumer readiness signal found — starting to send events.")
else:
    logger.warning(
        "Consumer readiness signal not found after %ds — sending anyway.",
        _READY_TIMEOUT_SECS,
    )

# -----------------------------------
# Job Widgets
# -----------------------------------
# dbutils is injected by Databricks — available in notebook and job context.
# Defaults match the local kafka_producer.py behaviour.
dbutils.widgets.text("num_events",       "500", "Total events to send (0 = run forever)")
dbutils.widgets.text("batch_size",       "10",  "Events per batch")
dbutils.widgets.text("interval_seconds", "5",   "Sleep between batches (seconds)")

# A spark_python_task passes job parameters as argv (--num_events=0), which widgets
# do not see; argv wins, the widget value (default or notebook input) is the fallback.
import sys as _sys


def _param(name: str) -> str:
    prefix = f"--{name}="
    for a in _sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix):]
    return dbutils.widgets.get(name)


num_events     = int(_param("num_events"))
batch_size     = int(_param("batch_size"))
interval_secs  = float(_param("interval_seconds"))

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
