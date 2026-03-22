# generate_sample_data.py
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# --- Sales transactions (intentionally includes dirty records) ---
divisions = ['011', '011', '011', '540', '011', '011', None, '011', '999', '011']
records = []
for i in range(500):
    txn_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))
    records.append({
        'transaction_id': f'TXN{i:04d}',
        'customer_id': f'CUST{random.randint(1, 100):03d}' if random.random() > 0.03 else None,
        'product_id': f'PROD{random.randint(1, 50):03d}',
        'division': random.choice(divisions),
        'quantity': random.randint(1, 20) if random.random() > 0.05 else -1,
        'unit_price': round(random.uniform(5.0, 500.0), 2) if random.random() > 0.04 else 0,
        'transaction_date': txn_date.strftime('%Y-%m-%d') if random.random() > 0.02 else '9999-99-99',
        'store_id': f'STORE{random.randint(1, 20):02d}',
        'coupon_applied': random.choice([True, False])
    })

df = pd.DataFrame(records)
df.to_csv(r'C:/Users/ameya/databricks-dqx-poc/data/sample/sales_transactions.csv', index=False)
print(f"Sales: {len(df)} rows generated")