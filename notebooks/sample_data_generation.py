# generate_sample_data.py

#Importing libraries
import pandas as pd                         # to Create & store dataset
import numpy as np                          # to introduce Controlled randomness
import random                               # to inject bad data
import io                                   # to write CSV to in-memory buffer for ADLS upload
from datetime import datetime, timedelta    # to generate relalistic transaction dates

#Fix Randomness - ensures reproducibility of the generated dataset across runs
random.seed(42)
np.random.seed(42)

# --- ADLS Configuration ---
ACCOUNT_NAME = "databricksdqxpocstorage"
FILE_SYSTEM = "databricks-dqx"
ADLS_PATH = f"abfss://{FILE_SYSTEM}@{ACCOUNT_NAME}.dfs.core.windows.net/data/sample/sales_transactions.csv"

# Define Division Values --- Sales transactions (intentionally includes dirty records) ---
# '011' - valid division, '540' - valid division, None - missing value, '999' - invalid code
#division_allowlist - from rules_batch.yml validates against this list,
divisions = ['011', '011', '011', '540', '011', '011', None, '011', '999', '011']   

# Create Empty Record List - we will populate this with generated transaction records
records = []
for i in range(500):                                                            # Generate 500 sample sales transaction records -> Loop = each row = one retail transaction
    txn_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))    # Generate random transaction date in 2024 
    records.append({                                                            # Build Each Record as a dictionary with the fields:
        'transaction_id': f'TXN{i:04d}',                                        # Unique transaction ID - TXN0000, TXN0001... - validates the rule "transaction_id_format" from rules_batch.yml
        'customer_id': f'CUST{random.randint(1, 100):03d}' if random.random() > 0.03 else None,         # 97% → valid customer_id & 3% → missing customer_id (None) - validates the rule "customer_id_format" from rules_batch.yml
        'product_id': f'PROD{random.randint(1, 50):03d}',                       # PROD001, PROD002... - 50 unique products  - validates the rule "product_id_format" from rules_batch.yml
        'division': random.choice(divisions),                                   # 90% → valid division & 10% → dirty values (None or '999') - validates the rule "division_allowlist" from rules_batch.yml
        'quantity': random.randint(1, 20) if random.random() > 0.05 else -1,    # 95% → valid quantity & 5% → negative quantity - validates the rule "quantity_positive" from rules_batch.yml
        'unit_price': round(random.uniform(5.0, 500.0), 2) if random.random() > 0.04 else 0,            # 96% → valid unit price & 4% → zero price - validates the rule "unit_price_positive" from rules_batch.yml
        'transaction_date': txn_date.strftime('%Y-%m-%d') if random.random() > 0.02 else '9999-99-99',  # 98% → valid transaction date & 2% → invalid date - validates the rule "transaction_date_format" from rules_batch.yml
        'store_id': f'STORE{random.randint(1, 20):02d}',                        # 95% → valid store_id & 5% → dirty store_id - validates the rule "store_id_format" from rules_batch.yml
        'coupon_applied': random.choice([True, False])                          # coupon_applied - 50% True, 50% False - validates the rule "coupon_applied_boolean" from rules_batch.yml
    })

# Convert the list of records into a DataFrame
df = pd.DataFrame(records)

# Write CSV to in-memory buffer and upload directly to ADLS
csv_buffer = io.StringIO()
df.to_csv(csv_buffer, index=False)
dbutils.fs.put(ADLS_PATH, csv_buffer.getvalue(), overwrite=True)

# Print summary of the generated dataset
print(f"Sales: {len(df)} rows generated and uploaded to {ADLS_PATH}")
