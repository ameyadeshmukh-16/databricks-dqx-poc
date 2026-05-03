"""
Kafka Producer — Azure Event Hubs Kafka Protocol

Simulates a continuous stream of retail POS sales events and sends them to
Azure Event Hubs using the Kafka protocol endpoint (SASL_SSL, port 9093).

Uses confluent-kafka (the officially recommended Python client for Azure Event Hubs).
kafka-python is NOT used here — it has a known incompatibility with Azure EH where
API version auto-detection causes a 60-second metadata timeout on the first send.

Data generation is identical to stream_simulator.py with the same bad-data
injection rates, so downstream DQX rules are exercised the same way:

  - 2% missing event_id         → triggers event_id_not_null rule
  - 6% negative quantity        → triggers quantity_positive rule
  - 4% zero unit_price          → triggers price_positive rule
  - Invalid divisions ("540" coupon mismatch, "999", None)
                                → triggers division_valid + coupon_integrity rules

Run this script locally (outside Databricks) while kafka_stream_dqx_validation.py
is active on the cluster to observe real-time clean/quarantine splitting.

Usage:
    python -m rtm_kafka.producer.kafka_producer
"""

# -----------------------------------
# Imports
# -----------------------------------
import json
import random
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Producer

# Import centralised Kafka config — SASL_SSL credentials for Azure EH
from rtm_kafka.config.kafka_config import KAFKA_PRODUCER_CONFIG, TOPIC

# -----------------------------------
# Environment
# -----------------------------------
load_dotenv()

# -----------------------------------
# Delivery Callback
# -----------------------------------
# confluent-kafka is asynchronous by default — delivery_callback is invoked
# once per message after the broker acknowledges (or rejects) it.
# This gives us per-message confirmation without blocking the send loop.
def delivery_callback(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")                  # Surface broker-side errors immediately

# -----------------------------------
# Kafka Producer
# -----------------------------------
# Producer() accepts the config dict directly — no keyword unpacking needed.
# Unlike kafka-python, instantiation here triggers the actual connection attempt,
# so a failure here means auth or network issues (not a lazy-connect false positive).
print("🚀 Initialising Kafka producer (Azure Event Hubs)...")

producer = Producer(KAFKA_PRODUCER_CONFIG)

print(f"📡 Target   : {KAFKA_PRODUCER_CONFIG['bootstrap.servers']}")
print(f"📡 Topic    : {TOPIC}")

# -----------------------------------
# Event Generator
# -----------------------------------
def generate_event(i):
    """
    Generate a single simulated POS sales event.

    The parameter 'i' seeds a zero-padded sequential event_id (e.g. EVT000042).
    Intentional bad data is injected at low rates to exercise DQX rule coverage:

      - event_id   : 2%  chance of None  (simulates missing primary key)
      - quantity   : 6%  chance of -1    (simulates negative stock error)
      - unit_price : 4%  chance of 0     (simulates unpriced item)
      - division   : includes "540", "999", None  (simulates invalid routing codes)
    """
    return {
        # 2% chance of missing event_id to simulate absent primary key
        "event_id":        f"EVT{i:06d}" if random.random() > 0.02 else None,

        # Store and product identifiers — always valid
        "store_id":        f"STORE{random.randint(1, 20):02d}",
        "product_id":      f"PROD{random.randint(1, 50):03d}",

        # Division includes intentionally invalid codes ("540" coupon mismatch, "999", None)
        # to exercise the division_valid and coupon_integrity DQX rules
        "division":        random.choice(["011", "012", "013", "540", None, "999"]),

        # 6% chance of negative quantity to trigger quantity_positive rule
        "quantity":        random.randint(1, 10) if random.random() > 0.06 else -1,

        # 4% chance of zero price to trigger price_positive rule
        "unit_price":      round(random.uniform(5, 300), 2) if random.random() > 0.04 else 0,

        "coupon_applied":  random.choice([True, False]),
        "event_timestamp": datetime.now().isoformat(),
    }

# -----------------------------------
# Simulation Loop
# -----------------------------------
# Sends batches of 10 events every 5 seconds — same cadence as stream_simulator.py.
# producer.poll(0) is called after each produce() to drain the delivery callback
# queue without blocking. producer.flush() after each batch ensures all messages
# are acknowledged before the sleep interval.
print("🚀 Kafka simulator started. Sending batches every 5 seconds...")
print("   Press Ctrl+C to stop.\n")

i = 0
try:
    while True:
        for _ in range(10):                                     # 10 events per batch
            producer.produce(
                TOPIC,
                value=json.dumps(generate_event(i)).encode("utf-8"),   # Explicit JSON bytes
                callback=delivery_callback                              # Per-message ack/error
            )
            producer.poll(0)                                    # Non-blocking callback drain
            i += 1                                             # Increment for unique event_id

        producer.flush()                                        # Wait for all in-flight messages to be acknowledged
        print(f"✅ Sent batch — total events dispatched: {i}")
        time.sleep(5)                                           # 5-second interval between batches

except KeyboardInterrupt:
    print("\n❌ Simulator stopped by user.")
    producer.flush()
    print("✅ Producer closed cleanly.")
