"""
Event Generator — single source of truth for simulated POS event data.

Extracted from kafka_producer.py to eliminate near-identical implementations
across stream_simulator.py, stream_simulator_adls.py, and kafka_producer.py.

All producers import generate_event() from here so bad-data injection rates
and field names stay in sync with the DQX rules in dq_rules_registry.
"""

import random
from datetime import datetime

from config.constants import (
    BAD_DATA_RATE_EVENT_ID,
    BAD_DATA_RATE_PRICE,
    BAD_DATA_RATE_QUANTITY,
    ALL_DIVISIONS,
)


def generate_event(i: int) -> dict:
    """
    Generate one simulated POS sales event.

    The parameter i seeds a zero-padded sequential event_id (e.g. EVT000042).
    Bad data is injected at controlled rates to exercise DQX rule coverage:

      event_id   : 2%  chance of None  → triggers event_id_not_null rule
      quantity   : 6%  chance of -1    → triggers quantity_positive rule
      unit_price : 4%  chance of 0     → triggers price_positive rule
      division   : includes "540", "999", None → triggers division_valid + coupon_integrity rules
    """
    return {
        "event_id":        f"EVT{i:06d}" if random.random() > BAD_DATA_RATE_EVENT_ID else None,
        "store_id":        f"STORE{random.randint(1, 20):02d}",
        "product_id":      f"PROD{random.randint(1, 50):03d}",
        "division":        random.choice(ALL_DIVISIONS),
        "quantity":        random.randint(1, 10) if random.random() > BAD_DATA_RATE_QUANTITY else -1,
        "unit_price":      round(random.uniform(5, 300), 2) if random.random() > BAD_DATA_RATE_PRICE else 0,
        "coupon_applied":  random.choice([True, False]),
        "event_timestamp": datetime.now().isoformat(),
    }
