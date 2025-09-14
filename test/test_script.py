#!/usr/bin/env python3

print("Starting test script...")

import pandas as pd
import duckdb

print("All imports successful!")

# Test basic functionality
print("Testing basic functionality...")

# Create a small dataset
date_range = pd.date_range(start='2024-01-01', end='2024-01-03', freq='D')
print(f"Date range created: {len(date_range)} days")

transactions = []
for i, date in enumerate(date_range):
    transactions.append({
        'date': date,
        'customer_number': 100000 + i,
        'product_name': f'Product_{i}',
        'amount': 10.0 + i
    })

print(f"Created {len(transactions)} transactions")

# Create DataFrame
df = pd.DataFrame(transactions)
print("DataFrame created successfully")

# Test DuckDB
try:
    con = duckdb.connect(database='test_db.db', read_only=False)
    con.execute("CREATE TABLE IF NOT EXISTS test_data AS SELECT * FROM df")
    result = con.execute("SELECT COUNT(*) FROM test_data").fetchone()
    print(f"Database test successful: {result[0]} records")
    con.close()
    print("✅ All tests passed!")
except Exception as e:
    print(f"❌ Database error: {e}")

print("Test script completed!")
