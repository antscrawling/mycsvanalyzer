import json
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import sys
from pprint import pprint


OUTPUT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # Ensure OUTPUT_ROOT points to 'csvanalyzer' folder
SRC_FILES = os.path.join(OUTPUT_ROOT, 'src')  # Path to src directory

#all input is in the src directory
BIG_CSV_FILES = os.path.join(OUTPUT_ROOT, 'src')

SPLIT_CSV_FOLDER = 'SPLITCSV'  #
#output of split csv files csvanalyzer/src/SPLITCSV/*_1.csv *_2.csv etc.
SPLIT_CSV_FILES = os.path.join(SRC_FILES, 'SPLITCSV')
#output of split parquet files csvanalyzer/src/PARQUET/*.parquet
PARQUET_FILES = os.path.join(OUTPUT_ROOT, 'src', 'PARQUET')
#output of split zlib files
ZLIB_FILES = os.path.join(OUTPUT_ROOT, 'src', 'ZLIB')


CSV_FILE_EXTENSION = 'csv' 
PARQUET_FILE_EXTENSION = 'parquet'
ZLIB_FILE_EXTENSION = 'zlib'

# Define output directories for split files
SPLIT_CSV_PATH = SPLIT_CSV_FILES  # Output CSV files in csvanalyzer/src/SPLITCSV
PARQUET_PATH = os.path.join(SRC_FILES, PARQUET_FILES)  # Output Parquet files in csvanalyzer/src/PARQUET
ZLIB_PATH = os.path.join(SRC_FILES, ZLIB_FILES)  # Output Zlib files in csvanalyzer/src/ZLIB

SOURCE_DIR = os.path.join(OUTPUT_ROOT,'src')
CITIES_JSON = os.path.join(SOURCE_DIR,'cities.json')
# DATA FILES
PARAM_DB = os.path.join(SOURCE_DIR, 'parameter.db')
ANALYZED_DB = os.path.join(SOURCE_DIR, 'analyzed_database.db')
CHATGPT_RESULTS = os.path.join(SOURCE_DIR, 'chatgpt_results.json')
ANALYZED_CSV = os.path.join(SOURCE_DIR, 'analyzed_data.csv')
SALES_TIMESERIES_CSV = os.path.join(SOURCE_DIR, 'sales_timeseries.csv')
SALES_TIMESERIES_DB = os.path.join(SOURCE_DIR, 'sales_timeseries.db')
# DATABASE

def generate_initial_data(chunks=10_000_000):
    print("🏪 Retail Sales Database Generator")
    print("=" * 40)

### DO NOT CONVERT ANYTHING TO INTEGER UNLESS IT IS A TRUE INTEGER  
## DO NOT CONVERT FLOATS TO STRINGS EITHER
    np.random.seed(142)

    # Create a date range with daily frequency (split into chunks)
    start_date = '1900-01-01'
    end_date = '2025-09-02'
    
    # Calculate full date range first to get total days
    full_range = pd.date_range(start=start_date, end=end_date, freq='D')
    total_days = len(full_range)

    # Adjust chunk size to represent 1 year per chunk
    chunks = (pd.to_datetime(end_date).year - pd.to_datetime(start_date).year) + 1
    chunk_size = total_days // chunks
    
    print(f"📅 Date range: {total_days} days (processing in {chunks} chunks)")

    # Load dependencies
    with open('src/cities.json') as f:
        cities = json.load(f)
    
    # Product catalog and other static data definitions remain the same
    genders = ["M", "F"]
    products = [
        {'product_id': 100, 'product_name': 'iPhone 13', 'unit_price': 999.00},
        {'product_id': 200, 'product_name': 'Samsung Galaxy S21', 'unit_price': 899.00},
        {'product_id': 300, 'product_name': 'Google Pixel 6', 'unit_price': 599.00},
        {'product_id': 400, 'product_name': 'OnePlus 9', 'unit_price': 729.00},
        {'product_id': 500, 'product_name': 'Xiaomi Mi 11', 'unit_price': 749.00},
        {'product_id': 600, 'product_name': 'Sony Xperia 5', 'unit_price': 899.00},
        {'product_id': 700, 'product_name': 'Oppo Find X3', 'unit_price': 1149.00},
        {'product_id': 800, 'product_name': 'Nokia 8.3', 'unit_price': 699.00},
        {'product_id': 900, 'product_name': 'Realme GT', 'unit_price': 599.00},
        {'product_id': 1000, 'product_name': 'Water', 'unit_price': 2.00},
        {'product_id': 1100, 'product_name': 'Sparkling Water', 'unit_price': 2.50},
        {'product_id': 1200, 'product_name': 'Iced Tea', 'unit_price': 3.00},
        {'product_id': 1300, 'product_name': 'MacBook Pro', 'unit_price': 2399.00},
        {'product_id': 1400, 'product_name': 'Dell XPS 13', 'unit_price': 1499.00},
        {'product_id': 1500, 'product_name': 'HP Spectre x360', 'unit_price': 1699.00},
        {'product_id': 1600, 'product_name': 'Lenovo ThinkPad X1', 'unit_price': 1899.00},
        {'product_id': 1700, 'product_name': 'iPad Pro', 'unit_price': 1099.00},
        {'product_id': 1800, 'product_name': 'Samsung Galaxy Tab S7', 'unit_price': 849.00},
        {'product_id': 1900, 'product_name': 'Microsoft Surface Pro 7', 'unit_price': 999.00},
        {'product_id': 2000, 'product_name': 'Amazon Kindle', 'unit_price': 89.00}
    ]
    transaction_types = ['Product Sale',
                         'Product Refund',
                         'Product Exchange'
                        ]
    
    # Initialize database
    con = duckdb.connect(database=SALES_TIMESERIES_DB, read_only=False)
    con.execute("DROP TABLE IF EXISTS sales_data")
    print("💾 Database initialized")
    
    # Create table schema first
    schema_sql = """
    CREATE TABLE sales_data (
        date varchar,
        transaction_id INTEGER,
        transaction_desc VARCHAR,
        customer_number INTEGER,
        age INTEGER,
        gender varchar,
        receipt_number INTEGER,
        product_id INTEGER,
        product_name varchar,
        units_sold INTEGER,
        unit_price_sgd DECIMAL(10,2),
        total_amount_per_product_sgd DECIMAL(10,2),
        receipt_total_sgd DECIMAL(10,2),
        country_id INTEGER,
        country varchar,
        city varchar,
        income DECIMAL(10,2)
    )
    """
    con.execute(schema_sql)
    
    # Process in chunks
    customer_id = 100001
    receipt_id = 200001
    customer_ages = {}
    
    # Process data in chunks
    for chunk_idx in range(chunks):
        chunk_start = chunk_idx * chunk_size
        chunk_end = (chunk_idx + 1) * chunk_size if chunk_idx < chunks - 1 else total_days
        
        # Get date range for this chunk
        date_chunk = full_range[chunk_start:chunk_end]
        
        print(f"\n🔄 Processing chunk {chunk_idx+1}/{chunks} ({len(date_chunk)} days)")
        
        # Generate transactions for this chunk
        transactions = []
        
        for date in tqdm(date_chunk, desc=f"Chunk {chunk_idx+1}/{chunks}", total=(pd.to_datetime(end_date).year - pd.to_datetime(start_date).year) + 1):
            # tqdm progress bar now reflects the total number of years between start_date and end_date
            
            
            
            # Number of receipts per day
            day_of_week = date.weekday()
            if day_of_week >= 5:  # Weekend
                num_receipts = np.random.randint(20, 40)
            else:  # Weekday
                num_receipts = np.random.randint(30, 60)
            
            # Generate receipts logic (same as before)
            for receipt in range(num_receipts):
                # Assign age to customer if not already assigned
                if customer_id not in customer_ages:
                    # Realistic age distribution: more customers in 25-45 range
                    age_ranges = [18, 25, 35, 45, 55, 65, 75]
                    age_weights = [0.05, 0.20, 0.25, 0.25, 0.15, 0.08, 0.02]
                    age_range_start = np.random.choice(age_ranges, p=age_weights)
                    customer_ages[customer_id] = np.random.randint(age_range_start, min(age_range_start + 10, 80))
                    city = np.random.choice([city['name'] for city in cities])
                    selected_city = next(item for item in cities if item['name'] == city)
                    country_id = selected_city['country_id']
                    country = selected_city['country']
                    transaction_type = transaction_types[np.random.choice([0,1,2], p=[0.95,0.025,0.025])]
                    incomes =[30_000,40_000,50_000,60_000,70_000,80_000,90_000,100_000,120_000,150_000,200_000]
                    customer_income = np.random.choice(range(min(incomes),max(incomes)),1)
                    customer_ages[customer_id] = customer_income

                # Number of items per receipt
                items_per_receipt = np.random.choice([1, 2, 3, 4], p=[0.4, 0.3, 0.2, 0.1])
                selected_products = np.random.choice(len(products), size=items_per_receipt, replace=False)
                
                receipt_total = 0
                
                for product_idx in selected_products:
                    product = products[product_idx]
                    units_sold = np.random.randint(1, 4)  # 1-3 units per item
                    
                    # Add some price variation (±10%)
                    unit_price = product['unit_price'] * np.random.uniform(0.9, 1.1)
                    
                    total_amount_per_product = units_sold * unit_price
                    receipt_total += total_amount_per_product
                    
                    # Add hour variation throughout the day
                    hour = np.random.randint(6, 22)  # Store hours 6 AM to 10 PM
                    transaction_datetime = date + timedelta(hours=hour, minutes=np.random.randint(0, 60))
                    transaction_id = np.random.randint(1000000, 9999999)    
                    transactions.append({
                        'date': transaction_datetime,
                        'transaction_id': transaction_id,
                        'transaction_desc': transaction_type,
                        'customer_number': customer_id,
                        'age': customer_ages[customer_id],
                        'gender': np.random.choice(genders),
                        'receipt_number': receipt,
                        'product_id': product['product_id'],
                        'product_name': product['product_name'],
                        'units_sold': units_sold,
                        'unit_price_sgd': round(unit_price, 2),
                        'total_amount_per_product_sgd': round(total_amount_per_product, 2),
                        'receipt_total_sgd': 0,  # Will be filled later
                        'country_id': country_id,
                        'country': country,
                        'city': city,
                        'income': customer_income
                    })
                
                # Update receipt total for all items in this receipt
                receipt_start_idx = len(transactions) - items_per_receipt
                for i in range(receipt_start_idx, len(transactions)):
                    transactions[i]['receipt_total_sgd'] = round(receipt_total, 2)
                
                customer_id += 1
                
            
            # After every 10 days, save to database to avoid memory issues
            if len(transactions) > 50000 or date == date_chunk[-1]:
                print(f"Saving {len(transactions)}, records to database...")
                df_chunk = pd.DataFrame(transactions)
                
                # Log the schema of df_view
                print("\n🔍 DataFrame schema before insertion:")
                print(df_chunk.dtypes)

                # Log the first few rows to identify problematic columns
                print("\n🔍 DataFrame preview before type conversion:")
                print(df_chunk.head())

                # Handle stringified dictionaries in columns
                for column in df_chunk.columns:
                    if df_chunk[column].apply(lambda x: isinstance(x, str) and x.startswith('{') and x.endswith('}')).any():
                        print(f"⚠️ Column '{column}' contains stringified dictionaries. Parsing and extracting values.")
                        df_chunk[column] = df_chunk[column].apply(
                            lambda x: json.loads(x).get('M', x) if isinstance(x, str) and x.startswith('{') and x.endswith('}') else x
                        )

                # Refine dictionary handling in the 'gender' column
                # Ensure 'gender' column contains CHARACTER MALE OR FEMALE
                if df_chunk['gender'].apply(lambda x: isinstance(x, dict)).any():
                  
                    df_chunk['gender'] = df_chunk['gender'].apply(lambda x: x.get('M', 0) if isinstance(x, dict) else x)
                
            
                if df_chunk['country_id'].apply(lambda x: isinstance(x, str) and x.startswith('C')).any():
                    print("⚠️ Refining 'country_id' column preprocessing to extract integer values.")
                    df_chunk['country_id'] = df_chunk['country_id'].apply(lambda x: int(x[1:]) if isinstance(x, str) and x.startswith('C') else x)

            
                for column in df_chunk.select_dtypes(include=['object']).columns:
                    df_chunk[column] = df_chunk[column].astype(str)

              
                for column in df_chunk.select_dtypes(include=['object']).columns:
                    df_chunk[column] = df_chunk[column].apply(lambda x: ''.join(filter(str.isprintable, str(x))) if isinstance(x, str) else x)

                # Correct data generation and insertion to DuckDB
                dtype_mapping = {
                    'int64': 'BIGINT',
                    'float64': 'DOUBLE',
                    'object': 'VARCHAR',
                    'string': 'VARCHAR',
                    'NoneType': 'VARCHAR',
                    'datetime64[ns]': 'TIMESTAMP',
                    'Boolean': 'BOOLEAN'
                }
                try:
                    # Dynamically create the table schema based on DataFrame dtypes
                    df_dtypes = {col: dtype_mapping.get(str(dtype), 'VARCHAR') for col, dtype in df_chunk.dtypes.items()}
                    schema_sql = "CREATE TABLE IF NOT EXISTS data (" + ", ".join(
                        f'"{col}" {dtype}' for col, dtype in df_dtypes.items() if col not in ['discount_period', 'discount_percentage', 'discount_applied']
                    ) + ")"
                    print("🔍 Generated schema:")
                    print(schema_sql)
                    con.execute(schema_sql)

                    # Drop discount-related fields from the DataFrame
                    df_chunk = df_chunk.drop(columns=['discount_period', 'discount_percentage', 'discount_applied'], errors='ignore')

                    # Convert nullable columns to use pd.NA for proper null handling
                    nullable_columns = df_chunk.columns[df_chunk.isnull().any()].tolist()
                    for column in nullable_columns:
                        df_chunk[column] = df_chunk[column].apply(lambda x: pd.NA if x == 'None' else x)

                    # Explicitly cast nullable columns to appropriate nullable data types
                    for column in nullable_columns:
                        if df_chunk[column].dtype == 'object':
                            df_chunk[column] = df_chunk[column].astype('Int64')

                    # Insert data into DuckDB
                    con.register('df_view', df_chunk)
                    insert_query = "INSERT INTO data SELECT * FROM df_view"
                    con.execute(insert_query)
                

                except Exception as e:
                    raise RuntimeError(f"❌ Error during database insertion: {e}")
                
           
        transactions = []  # Clear for next batch
        print(f"✅ Chunk {chunk_idx+1}/{chunks} completed")
    
    # Create indexes after all data is inserted
    print("\n📊 Creating indexes...")
    con.execute("CREATE INDEX idx_date ON data (date)")
    con.execute("CREATE INDEX idx_customer ON data (customer_number)")
    con.execute("CREATE INDEX idx_receipt ON data (receipt_number)")
    con.execute("CREATE INDEX idx_product ON data (product_id)")
    
    # Get statistics about the table
    print("\n📈 Database statistics:")
    record_count = con.execute("SELECT COUNT(*) from sales_data").fetchone()[0]
    revenue = con.execute("SELECT SUM(total_amount_per_product_sgd) from sales_data").fetchone()[0]
    unique_customers = con.execute("SELECT COUNT(DISTINCT customer_number) from sales_data").fetchone()[0]
    unique_receipts = con.execute("SELECT COUNT(DISTINCT receipt_number) from sales_data").fetchone()[0]
    
    con.close()
    
    print("\n✅ Data saved to sales_timeseries.db database file")
    print(f"📊 Total records: {record_count:,}")
    print(f"💰 Total revenue: SGD ${revenue:,.2f}")
    print(f"👥 Unique customers: {unique_customers:,}")
    print(f"🧾 Unique receipts: {unique_receipts:,}")
    print("🎉 Database creation complete!")

def main():
    print("🏪 Retail TimeSeries Database Generator")
    print("=" * 40)

    while True:
        print("\n🔄 Options:")
        print("   1️⃣  Generate or recreate database")
        print("   2️⃣  Launch retail menu")
        print("   3️⃣  Run CSV Analyzer Menu")
        print("   4️⃣  Save entire DataFrame to CSV")
        print("   0️⃣  Exit")

        choice = input("\nSelect option (0-5): ").strip()

        if choice == '1':
            if os.path.exists(SALES_TIMESERIES_DB):
                os.remove(SALES_TIMESERIES_DB)
                print("🗑️  Deleted existing database")
            print("⚡ Generating new database...")
            generate_initial_data()
            print("✅ Database generation complete!")
        elif choice == '2':
            try:
                from retail_menu import RetailMenu
                menu = RetailMenu()
                menu.main_menu()
            except ImportError:
                print("❌ retail_menu.py not found!")
        elif choice == '3':
            try:
                from analyze_csv import main as analyze_csv_main
                analyze_csv_main(country_codes={})
            except ImportError:
                print("❌ analyze_csv.py not found or has errors!")
        elif choice == '4':
            try:
                # Update the file path for saving analyzed_data.csv
                print("💾 Saving entire DataFrame to src/analyzed_data.csv...")
                with duckdb.connect(SALES_TIMESERIES_DB, read_only=True) as con:
                    df = con.execute("SELECT * FROM sales_data").df()
                    with tqdm(total=len(df), desc="Saving rows") as pbar:
                        df.to_csv(SALES_TIMESERIES_CSV, index=False, chunksize=1000)
                        pbar.update(len(df))
                print("✅ DataFrame saved successfully to src/analyzed_data.csv!")
            except Exception as e:
                print(f"❌ Error saving DataFrame to CSV: {e}")
        elif choice == '0':
            print("👋 Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Please select a valid option.")


if __name__ == "__main__":
   #mydict =  globals()
   #pprint(mydict)
    main()
    