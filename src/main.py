import json
import pandas as pd
import numpy as np
import duckdb
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
import os, time
import sys
from pprint import pprint
from concurrent.futures import ProcessPoolExecutor, as_completed
from load_csv_to_df import load_csv_to_df


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

def save_to_duckdb(df_all:pd.DataFrame, db_path:str=SALES_TIMESERIES_DB):
    if os.path.exists(db_path):
        os.remove(db_path)
        print("🗑️  Deleted existing database")
    con = duckdb.connect(database=db_path, read_only=False)
    for column in tqdm(df_all.select_dtypes(include=['object']).columns):
        df_all[column] = df_all[column].apply(lambda x: ''.join(filter(str.isprintable, str(x))) if isinstance(x, str) else x)
        # Insert all data
        
        con.register('df_view', df_all)
        insert_query = "INSERT INTO sales_data SELECT * FROM df_view"
        con.execute(threads=4)
        con.execute(insert_query)
        # Create indexes after all data is inserted
        print("\n📊 Creating indexes...")
        con.execute("CREATE INDEX idx_date ON sales_data (date)")
        con.execute("CREATE INDEX idx_customer ON sales_data (customer_number)")
        con.execute("CREATE INDEX idx_receipt ON sales_data (receipt_number)")
        con.execute("CREATE INDEX idx_product ON sales_data (product_id)")
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
    record_count = con.execute("SELECT COUNT(*) from sales_data").fetchone()[0]
    revenue = con.execute("SELECT SUM(total_amount_per_product_sgd) from sales_data").fetchone()[0]
    unique_customers = con.execute("SELECT COUNT(DISTINCT customer_number) from sales_data").fetchone()[0]
    unique_receipts = con.execute("SELECT COUNT(DISTINCT receipt_number) from sales_data").fetchone()[0]
    con.close()

def generate_initial_data(start_iteration:str,end_iteration:str):
    print("🏪 Retail Sales Database Generator")
    print("=" * 40)

    np.random.seed(142)

    # Create a date range with daily frequency (split into chunks)
    start_date = start_iteration
    end_date = end_iteration

    # Calculate full date range first to get total days
    full_range = pd.date_range(start=start_date, end=end_date, freq='D')
    

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
    
    # Database tasks moved outside the loop
    print("💾 Database will be initialized after data generation.")
    
    # Process in chunks
    customer_id = 100001
    receipt_id = 200001
    transaction_id = 300001
    
    # Define the header as a dictionary
    header = {
        'date': 'date',
        'transaction_id': 'transaction_id',
        'transaction_desc': 'transaction_desc',
        'customer_number': 'customer_number',
        'age': 'age',
        'gender': 'gender',
        'receipt_number': 'receipt_number',
        'product_id': 'product_id',
        'product_name': 'product_name',
        'units_sold': 'units_sold',
        'unit_price_sgd': 'unit_price_sgd',
        'total_amount_per_product_sgd': 'total_amount_per_product_sgd',
        'receipt_total_sgd': 'receipt_total_sgd',
        'country_id': 'country_id',
        'country': 'country',
        'city': 'city',
        'income': 'income'
    }

    all_transactions = []
    all_transactions.append(header)  # Add header as the first row

    # Process data in chunks
    all_transactions = []

    #add multithreading
    # Use tqdm to show progress bar for the entire date range  
     
    for date in tqdm(full_range):            # tqdm progress bar now reflects the total number of years between start_date and end_date                                                        # Realistic age distribution: more customers in 25-45 range
        
        #build age
        age_ranges = [18, 25, 35, 45, 55, 65, 75]
        age_weights = [0.05, 0.20, 0.25, 0.25, 0.15, 0.08, 0.02]
        age_range_start = np.random.choice(age_ranges, p=age_weights)
        age = np.random.randint(age_range_start, min(age_range_start + 10, 80))
        
        #build city
        city = np.random.choice([city['name'] for city in cities])
        selected_city = next(item for item in cities if item['name'] == city)
        
        #build country 
        country_id = selected_city['country_id']
        country = selected_city['country']
        
        transaction_type = transaction_types[np.random.choice([0,1,2], p=[0.95,0.025,0.025])]
        
        #build income
        incomes =[30_000,40_000,50_000,60_000,70_000,80_000,90_000,100_000,120_000,150_000,200_000]
        income = int(np.random.choice(range(min(incomes), max(incomes))))
        
        
        #build product 
        product_id =1
        product = np.random.choice([p['product_name'] for p in products])
        
        units_sold = np.random.randint(1, 14)  # 1-3 units per item
        unit_price =  np.random.choice([2345,3398,1234,2234,678,890,456,234,567,890,345,1234,5678,2345 ,3787 ])
        #unit_price =  np.random.choice([p['unit_price'] for p in products])    
        total_amount_per_product = units_sold * unit_price
        receipt_total = 0
                    
                    # Add hour variation throughout the day
        hour = np.random.randint(6, 22)  # Store hours 6 AM to 10 PM
        minute = np.random.randint(0, 60)
        second = np.random.randint(0, 60)
        transaction_datetime = date + timedelta(hours=hour, minutes=minute, seconds=second)
        transaction_id = np.random.randint(1000000, 9999999)
        all_transactions.append({
            'date': transaction_datetime,
            'transaction_id': transaction_id,
            'transaction_desc': transaction_type,
            'customer_number': customer_id,
            'age': age,
            'gender': np.random.choice(genders),
            'receipt_number': f'{receipt_id},{transaction_id}',
            'product_id': product_id,
            'product_name': product,
            'units_sold': units_sold,
            'unit_price_sgd': round(unit_price, 2),
            'total_amount_per_product_sgd': round(total_amount_per_product, 2),
            'receipt_total_sgd': 0,  # Will be filled later
            'country_id': country_id,
            'country': country,
            'city': city,
            'income': income
        })
                
                # Update receipt total for all items in this receipt
              #  receipt_start_idx = len(transactions) - items_per_receipt
             #  for i in range(receipt_start_idx, len(transactions)):
             #      transactions[i]['receipt_total_sgd'] = round(receipt_total, 2)
                
        customer_id += 1
        receipt_id += 1
        transaction_id += 1
    
    # At the end, instead of saving to pickle, just return the list:
    return all_transactions
            
            # After every 10 days, save to database to avoid memory issues                    

def initialize_database():
    # After all chunks, process and insert into database
    print("\n💾 Initializing and saving to database...")
    con = duckdb.connect(database=SALES_TIMESERIES_DB, read_only=False)
    con.execute("DROP TABLE IF EXISTS sales_data")
    schema_sql = """
    CREATE TABLE sales_data (
        date TIMESTAMP,
        transaction_id VARCHAR,
        transaction_desc VARCHAR,
        customer_number VARCHAR,
        age INTEGER,
        gender VARCHAR,
        receipt_number VARCHAR,
        product_id VARCHAR,
        product_name VARCHAR,
        units_sold INTEGER,
        unit_price_sgd DECIMAL(10,2),
        total_amount_per_product_sgd DECIMAL(10,2),
        receipt_total_sgd DECIMAL(10,2),
        country_id VARCHAR,
        country VARCHAR,
        city VARCHAR,
        income DECIMAL(10,2)
    )
    """
    con.execute(schema_sql)
    
    
def load_dataset():
    all_transactions = []
    with open('src/sales_timeseries.pickle' , 'rb') as f:
        all_transactions = pickle.load(f)
    df_all = pd.DataFrame(all_transactions)    
    return df_all                                                          
 
def split_date_range(start_date, end_date, num_parts):
    full_range = pd.date_range(start=start_date, end=end_date, freq='D')
    ranges = []
    chunk_size = len(full_range) // num_parts
    for i in range(num_parts):
        chunk_start = full_range[i * chunk_size]
        if i == num_parts - 1:
            chunk_end = full_range[-1]
        else:
            chunk_end = full_range[(i + 1) * chunk_size - 1]
        ranges.append((chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
    return ranges
 
    
def main():
    df_all = load_dataset()  # Initialize df_all to avoid reference issues later
    print("🏪 Retail TimeSeries Database Generator")
    print("=" * 40)

    while True:
        print("\n🔄 Options:")
        print("   1️⃣  Generate sample dataset")
        print("    2. Load the dataset online (from run dataset)")
        print("    3. Save the dataset to database")
        print("    4. Display dataset")
        print("    0. Exit")

        choice = input("\nSelect option (0-5): ").strip()

        if choice == '1':
            start = input("Enter start date (YYYY-MM-DD) [default: 1900-01-01]: ").strip() or '1900-01-01'
            end = input("Enter end date (YYYY-MM-DD) [default: 2025-09-02]: ").strip() or '2025-09-02'
            print("⚡ Generating new database...")
            if os.path.exists(SALES_TIMESERIES_DB):
                os.remove(SALES_TIMESERIES_DB)
                print("🗑️  Deleted existing database")
            print("⚡ Generating new database...")

            # Split the date range into 5 parts
            date_ranges = split_date_range(start, end, 5)
            all_transactions = []
            with ProcessPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(generate_initial_data, dr[0], dr[1]) for dr in date_ranges]
                for fut in as_completed(futures):
                    result = fut.result()
                    if result:
                        all_transactions.extend(result)
            # Save combined results
            with open('src/sales_timeseries.pickle', 'wb') as f:
                pickle.dump(all_transactions, f)
            print("✅ Database generation complete!")
        elif choice == '2':
            print("🔄 Loading dataset from pickle file...")
            if not os.path.exists('src/sales_timeseries.pickle'):
                print("❌ sales_timeseries.pickle file not found. Please generate the dataset first.")
                 # Initialize an empty DataFrame
            else:
                with open('src/sales_timeseries.pickle', 'rb') as f:
                    df_all = pickle.load(f)
            print('Data loaded successfully from pickle file.')
        
        
        elif choice == '3':
            print('💾 Saving dataset to DuckDB database...')
            if os.path.exists(SALES_TIMESERIES_DB):
                os.remove(SALES_TIMESERIES_DB)
                print("🗑️  Deleted existing database")
            else:
                load_dataset()
            initialize_database()
            save_to_duckdb(df_all)

        elif choice == '4':
            print("📊 Displaying dataset...")
            df_all = load_dataset()
            print(df_all.head())                                                                                    
        elif choice == '0':
            print("👋 Goodbye!")
            sys.exit(0)
        else:
            print("❌ Invalid choice. Please select a valid option.")


if __name__ == "__main__":
    main()
