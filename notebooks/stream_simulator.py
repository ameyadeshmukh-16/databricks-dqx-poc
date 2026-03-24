# notebooks/stream_simulator.py
# Run locally: python -m notebooks.stream_simulator
# Keep running while streaming notebook is active

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv
import os, json, random, time
from datetime import datetime, timezone

load_dotenv()

CONN_STR = os.environ["EVENTHUB_CONN_STR"]
EVENTHUB_NAME = "retail-pos-stream"

divisions = ['011', '011', '540', '011', None, '013', '011', '999']

def generate_event(i: int) -> dict:
    return {
        "event_id": f"EVT{i:06d}" if random.random() > 0.02 else None,
        "store_id": f"STORE{random.randint(1, 20):02d}",
        "product_id": f"PROD{random.randint(1, 50):03d}",
        "division": random.choice(divisions),
        "quantity": random.randint(1, 10) if random.random() > 0.06 else -1,
        "unit_price": round(random.uniform(5.0, 300.0), 2) if random.random() > 0.04 else 0.0,
        "coupon_applied": random.choice([True, False]),
        "event_timestamp": datetime.now(timezone.utc).isoformat()
    }

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONN_STR,
    eventhub_name=EVENTHUB_NAME
)

print("Simulator running — sending events every 10 seconds. Ctrl+C to stop.")
i = 0
while True:
    batch = producer.create_batch()
    for _ in range(15):
        batch.add(EventData(json.dumps(generate_event(i))))
        i += 1
    producer.send_batch(batch)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent batch of 15 events (total: {i})")
    time.sleep(10)