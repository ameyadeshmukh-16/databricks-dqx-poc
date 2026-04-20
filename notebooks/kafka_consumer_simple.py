from kafka import KafkaConsumer
import json

# ✅ DQX-style rules
rules = [
    {"name": "division_not_null", "check": lambda r: r.get("division") is not None},
    {"name": "quantity_positive", "check": lambda r: r.get("quantity", 0) > 0},
    {"name": "price_positive", "check": lambda r: r.get("unit_price", 0) > 0},
]

def apply_dqx_rules(record):
    failed_rules = []

    for rule in rules:
        if not rule["check"](record):
            failed_rules.append(rule["name"])

    return failed_rules


consumer = KafkaConsumer(
    'retail-pos-stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

print("🚀 Kafka + DQX Validation Started...\n")

for message in consumer:
    data = message.value

    failed = apply_dqx_rules(data)

    if failed:
        print(f"❌ REJECT | Rules Failed: {failed} | Data: {data}")
    else:
        print(f"✅ CLEAN | Data: {data}")