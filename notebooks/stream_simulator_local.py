import json
import random
import time
import os
from datetime import datetime, timezone

print("DEBUG: Script started")
# -------------------------------
# Output Directory
# -------------------------------
OUTPUT_DIR = "data/stream_input"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------------------
# Sample Data Variations
# -------------------------------
divisions = ['011', '011', '540', '011', None, '013', '011', '999']

# -------------------------------
# Event Generator
# -------------------------------
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

# -------------------------------
# Streaming Simulation Loop
# -------------------------------
print("🚀 Stream Simulator Started")
print(f"Writing files to: {OUTPUT_DIR}")
print("Press CTRL+C to stop\n")

i = 0

while True:
    file_path = os.path.join(OUTPUT_DIR, f"batch_{i}.json")

    with open(file_path, "w") as f:
        for _ in range(15):
            f.write(json.dumps(generate_event(i)) + "\n")
            i += 1

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Created {file_path}")

    time.sleep(5)