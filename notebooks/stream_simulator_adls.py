# Stream Simulator (FINAL STABLE)

import json
import random
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

print("🚀 ADLS Stream Simulator Started")

load_dotenv()

ACCOUNT_NAME = "databricksdqxpocstorage"
FILE_SYSTEM = "databricks-dqx"
DIRECTORY = "stream_input"

service_client = DataLakeServiceClient(
    account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
    credential=os.environ["AZURE_STORAGE_KEY"]
)

file_system_client = service_client.get_file_system_client(FILE_SYSTEM)

# Ensure directory exists
directory_client = file_system_client.get_directory_client(DIRECTORY)

try:
    directory_client.get_directory_properties()
except Exception:
    directory_client.create_directory()

print(f"✅ Writing to ADLS: {DIRECTORY}")

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

batch_id = 0
i = 0

print("📡 Sending data every 30 seconds...\n")

while True:
    file_name = f"batch_{batch_id}.json"
    file_path = f"{DIRECTORY}/{file_name}"

    records = [json.dumps(generate_event(i + j)) for j in range(15)]
    i += 15

    file_content = "\n".join(records)

    file_client = file_system_client.get_file_client(file_path)

    try:
        file_client.delete_file()
    except Exception:
        pass  # file may not exist

    file_client.create_file()

    data_bytes = file_content.encode("utf-8")

    file_client.append_data(data=data_bytes, offset=0, length=len(data_bytes))
    file_client.flush_data(len(data_bytes))

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch {batch_id} uploaded")

    batch_id += 1
    time.sleep(30)