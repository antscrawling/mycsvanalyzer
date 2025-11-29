import json
import duckdb
import antigravity
from datetime import datetime, timedelta
import random
from tqdm import tqdm
import os, time, glob
import sys
from pprint import pprint
from concurrent.futures import ProcessPoolExecutor, as_completed
from load_csv_to_df import load_csv_to_df
from retail_menu import RetailMenu
from io import StringIO


OUTPUT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # Ensure OUTPUT_ROOT points to 'csvanalyzer' folder
SRC_FILES = os.path.join(OUTPUT_ROOT, 'src')  # Path to src directory

#all input is in the src directory
BIG_CSV_FILES = os.path.join(OUTPUT_ROOT, 'src')

SPLIT_CSV_FOLDER = 'SPLITCSV'  #
#output of split csv files csvanalyzer/src/SPLITCSV/*_1.csv *_2.csv etc.
SPLIT_CSV_FILES = os.path.join(SRC_FILES, 'SPLITCSV')
#output of split zlib files
ZLIB_FILES = os.path.join(OUTPUT_ROOT, 'src', 'ZLIB')
# Deprecated: PARQUET functionality removed but kept for legacy compatibility
PARQUET_FILES = os.path.join(OUTPUT_ROOT, 'src', 'DEPRECATED_PARQUET')

CSV_FILE_EXTENSION = 'csv' 
ZLIB_FILE_EXTENSION = 'zlib'

# Define standard chunk size for data processing (records per chunk)
CHUNK_SIZE = 1000  # Reduced from 6000 for better memory management

# Memory management constants
MAX_MEMORY_BATCH_SIZE = 500   # Process 500 records at a time to reduce memory usage
MAX_TRANSACTIONS_PER_HOUR = 5  # Limit transactions per hour (was 10)
MAX_HOURS_PER_CHUNK = 24      # Process max 24 hours at a time

# Define output directories for split files
SPLIT_CSV_PATH = SPLIT_CSV_FILES  # Output CSV files in csvanalyzer/src/SPLITCSV
ZLIB_PATH = ZLIB_FILES  # Output Zlib files in csvanalyzer/src/ZLIB

SOURCE_DIR = os.path.join(OUTPUT_ROOT,'src')
CITIES_JSON = os.path.join(SOURCE_DIR,'cities.json')
# DATA FILES
PARAM_DB = os.path.join(SOURCE_DIR, 'parameter.db')
ANALYZED_DB = os.path.join(SOURCE_DIR, 'analyzed_database.db')
CHATGPT_RESULTS = os.path.join(SOURCE_DIR, 'chatgpt_results.json')
ANALYZED_CSV = os.path.join(SOURCE_DIR, 'analyzed_data.csv')
SALES_TIMESERIES_CSV = os.path.join(SOURCE_DIR, 'sales_timeseries.csv')
SALES_TIMESERIES_DB = os.path.join(SOURCE_DIR, 'sales_timeseries.db')
SALES_TIMESERIES_PICKLE = os.path.join(SOURCE_DIR,'sales_timeseries.pickle')
# DATABASE


# Memory-efficient helper functions



def cleanup_temp_files(temp_files):
    """Remove temporary files and directory"""
    import os
    import shutil
    
    for temp_file in temp_files:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception as e:
            print(f"⚠️ Error removing {temp_file}: {e}")
    
    # Remove temp directory if empty
    temp_dir = os.path.join(PARQUET_FILES, 'temp')
    try:
        if os.path.exists(temp_dir) and not os.listdir(temp_dir):
            os.rmdir(temp_dir)
    except Exception as e:
        print(f"⚠️ Error removing temp directory: {e}")

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        return memory_mb
    except ImportError:
        print("📈 Memory monitoring unavailable (psutil not installed)")
        return 0
    except Exception as e:
        print(f"⚠️ Memory monitoring error: {e}")
        return 0

def is_valid_date(mydate: str) -> bool:
    """Check if date is valid in YYYY-MM-DD format (time is optional)"""
    try:
        # Try full datetime format first
        datetime.strptime(mydate, "%Y-%m-%d %H:%M:%S")
        return True
    except ValueError:
        try:
            # Try date-only format
            datetime.strptime(mydate, "%Y-%m-%d")
            return True
        except ValueError:
            return False

def ask_parameters():
    while True:
        start_input = input("Enter start date (YYYY-MM-DD) [default: 1900-01-01]: ").strip()
        end_input = input("Enter end date (YYYY-MM-DD) [default: today's date]: ").strip()
        print("⚡ Generating new database with memory-efficient processing...")
        
        # Show initial memory usage
        initial_memory = get_memory_usage()
        if initial_memory > 0:
            print(f"📊 Initial memory usage: {initial_memory:.1f} MB")

        # Handle start date
        if start_input:
            if is_valid_date(start_input):
                # If it's just a date (YYYY-MM-DD), add default time
                if len(start_input) == 10:  # Just date format
                    start = datetime.strptime(start_input + " 00:00:00", "%Y-%m-%d %H:%M:%S")
                else:  # Full datetime format
                    start = datetime.strptime(start_input, "%Y-%m-%d %H:%M:%S")
            else:
                print("❌ Invalid start date format. Please use YYYY-MM-DD")
                continue
        else:
            # Default start date
            start = datetime(1900, 1, 1, 0, 0, 0)

        # Handle end date
        if end_input:
            if is_valid_date(end_input):
                # If it's just a date (YYYY-MM-DD), add default time
                if len(end_input) == 10:  # Just date format
                    end = datetime.strptime(end_input + " 00:00:00", "%Y-%m-%d %H:%M:%S")
                else:  # Full datetime format
                    end = datetime.strptime(end_input, "%Y-%m-%d %H:%M:%S")
            else:
                print("❌ Invalid end date format. Please use YYYY-MM-DD")
                continue
        else:
            # Default to today's date
            end = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # Validate that start date is before end date
        if start >= end:
            print("❌ Start date must be before end date. Please try again.")
            continue

        # Split the datetime range into parts with manageable chunks
        date_ranges = split_hourly_range(start, end)
        
        # Warn user about large date ranges
        total_hours = (end - start).total_seconds() / 3600
        estimated_transactions = total_hours * MAX_TRANSACTIONS_PER_HOUR
        
        if estimated_transactions > 100000:  # More than 100k transactions
            print(f"⚠️ Warning: Large dataset detected!")
            print(f"📈 Estimated transactions: {estimated_transactions:,.0f}")
            print(f"📏 This will create {len(date_ranges)} processing chunks")
            confirm = input("Continue? (y/n): ").strip().lower()
            if confirm != 'y':
                continue
        
        return date_ranges


def generate_initial_data1(date_ranges:object, is_initial_generation:bool):
    """Generate data directly to DuckDB database - no parquet intermediary"""
    
    print(f'📊 Generating data for {len(date_ranges)} date ranges directly to DuckDB')
    
    # Initialize the database table once (will be created by first save_to_duckdb_table call)
    print("🔧 Database table will be created automatically on first data save...")
    
    # Process chunks sequentially to avoid database locking issues
    total_rows = 0
    print("🔄 Processing chunks sequentially to avoid database conflicts...")
    
    for i, (start_date, end_date) in enumerate(date_ranges):
        print(f"📊 Processing chunk {i+1}/{len(date_ranges)}: {start_date} to {end_date}")
        
        # Generate data and save directly to DuckDB
        chunk_result = generate_initial_data2(str(start_date), str(end_date), True, SALES_TIMESERIES_DB)
        
        if chunk_result and isinstance(chunk_result, dict):
            rows = chunk_result.get('total_transactions', 0)
            total_rows += rows
            print(f"✅ Processed chunk {i+1}: {rows:,} transactions saved to DuckDB")
        elif chunk_result:
            # Handle in-memory data (fallback)
            rows = len(chunk_result) if hasattr(chunk_result, '__len__') else 0
            total_rows += rows
            print(f"✅ Processed chunk {i+1}: {rows} rows")

    print(f"🎉 Database generation complete! {total_rows:,} total rows saved directly to DuckDB")
    
    # Load the data into memory for display if requested
    if is_initial_generation:
        print("🔄 Loading data from DuckDB into memory for display...")
        return load_dataset_from_duckdb()
    
    return None
    
def save_to_duckdb_table(source_con, table_name, db_path:str=SALES_TIMESERIES_DB):
    """Save data from a DuckDB table to persistent database using pure DuckDB operations"""
    import tempfile
    import os
    import time
    
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            # Use a short-lived connection to avoid lock conflicts
            target_con = duckdb.connect(database=db_path, read_only=False)
            
            try:
                # Ensure the sales_data table exists
                target_con.execute("""
                    CREATE TABLE IF NOT EXISTS sales_data (
                        date TIMESTAMP,
                        transaction_id VARCHAR,
                        transaction_desc VARCHAR,
                        customer_id VARCHAR,
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
                """)
                
                # Direct data transfer using DuckDB ATTACH/DETACH
                try:
                    # Create a temporary in-memory table from source data
                    source_data = source_con.execute(f"SELECT * FROM {table_name}").fetchall()
                    columns = [desc[1] for desc in source_con.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
                    
                    if source_data:
                        # Insert data directly
                        for row in source_data:
                            placeholders = ', '.join(['?' for _ in columns])
                            target_con.execute(f"INSERT INTO sales_data VALUES ({placeholders})", row)
                    
                    print(f"✅ Data chunk saved to {db_path}")
                    return  # Success, exit function
                    
                except Exception as inner_e:
                    print(f"❌ Error during direct data transfer: {inner_e}")
                    return
                        
            finally:
                # Always close the connection
                target_con.close()
                
        except Exception as e:
            if "lock" in str(e).lower() and attempt < max_retries - 1:
                print(f"⚠️ Database locked, retrying in {retry_delay} seconds... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                print(f"❌ Error saving chunk to database: {e}")
                return

def save_to_duckdb(data_source, db_path:str=SALES_TIMESERIES_DB):
    print("📊 Saving data to DuckDB...")
    
    # Ensure all required columns exist
    required_columns = [
        'date', 'transaction_id', 'transaction_desc', 'customer_id', 
        'age', 'gender', 'receipt_number', 'product_id', 'product_name', 
        'units_sold', 'unit_price_sgd', 'total_amount_per_product_sgd', 
        'receipt_total_sgd', 'country_id', 'country', 'city', 'income'
    ]
    
    # Use DuckDB to handle data processing
    with duckdb.connect() as temp_con:
        # Register the data source
        if hasattr(data_source, 'df'):
            temp_con.register('source_data', data_source.df())
        else:
            temp_con.register('source_data', data_source)
        
        # Check and add missing columns using SQL
        existing_cols = temp_con.execute("PRAGMA table_info('source_data')").fetchall()
        existing_col_names = [col[1] for col in existing_cols]
        
        select_parts = []
        for col in required_columns:
            if col in existing_col_names:
                select_parts.append(col)
            else:
                print(f"⚠️ Adding missing column: {col}")
                select_parts.append(f"'default_{col}' as {col}")
        
        # Create a clean dataset with all required columns
        clean_sql = f"SELECT {', '.join(select_parts)} FROM source_data"
        df_all = temp_con.execute(clean_sql).df()
    
    # Sanitize data types using DuckDB SQL
    print("🧹 Sanitizing data types...")
    
    with duckdb.connect() as temp_con:
        temp_con.register('clean_data', df_all)
        
        # Clean and convert data types using SQL
        sanitize_sql = """
        SELECT 
            TRY_CAST(date AS TIMESTAMP) as date,
            CAST(transaction_id AS VARCHAR) as transaction_id,
            CAST(transaction_desc AS VARCHAR) as transaction_desc,
            CAST(customer_id AS VARCHAR) as customer_id,
            COALESCE(TRY_CAST(age AS INTEGER), 0) as age,
            CAST(gender AS VARCHAR) as gender,
            CAST(receipt_number AS VARCHAR) as receipt_number,
            CAST(product_id AS VARCHAR) as product_id,
            CAST(product_name AS VARCHAR) as product_name,
            COALESCE(TRY_CAST(units_sold AS INTEGER), 0) as units_sold,
            COALESCE(TRY_CAST(unit_price_sgd AS DECIMAL(10,2)), 0.0) as unit_price_sgd,
            COALESCE(TRY_CAST(total_amount_per_product_sgd AS DECIMAL(10,2)), 0.0) as total_amount_per_product_sgd,
            COALESCE(TRY_CAST(receipt_total_sgd AS DECIMAL(10,2)), 0.0) as receipt_total_sgd,
            CAST(country_id AS VARCHAR) as country_id,
            CAST(country AS VARCHAR) as country,
            CAST(city AS VARCHAR) as city,
            COALESCE(TRY_CAST(income AS DECIMAL(10,2)), 0.0) as income
        FROM clean_data
        """
        df_all = temp_con.execute(sanitize_sql).df()
    
    # Data conversion is now handled in the sanitize step above
    
    # Reset all amount columns to zero as specified
    for col in ['unit_price_sgd', 'total_amount_per_product_sgd', 'receipt_total_sgd']:
        if col in df_all.columns:
            df_all[col] = 0.0
    
    # Income conversion is now handled in the sanitize step above
    
    # Country fields as VARCHAR - already handled by the string conversion above
    
    # Date conversion is now handled in the sanitize step above
    
    # Only keep the required columns in the right order
    df_all = df_all[required_columns]
    
    # Debug information
    print(f"DataFrame columns: {list(df_all.columns)}")
    print(f"Number of columns: {len(df_all.columns)}")
    
    # Create the database and table using manual schema definition instead of inference
    with duckdb.connect(database=db_path, read_only=False) as con:
        # First drop the table if it exists
        con.execute("DROP TABLE IF EXISTS sales_data")
        
        # Create the table with explicit schema
        schema_sql = """
        CREATE TABLE sales_data (
            date TIMESTAMP,
            transaction_id VARCHAR,
            transaction_desc VARCHAR,
            customer_id VARCHAR,
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
        
        # Insert data in batches to handle large datasets
        print("📥 Inserting data into DuckDB...")
        batch_size = 10000
        total_rows = len(df_all)
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df_all.iloc[start_idx:end_idx]
            
            # Register the batch as a view
            con.register('batch_view', batch_df)
            
            # Insert the batch
            try:
                con.execute("INSERT INTO sales_data SELECT * FROM batch_view")
                print(f"✅ Inserted rows {start_idx} to {end_idx}")
            except Exception as e:
                print(f"❌ Error inserting batch {start_idx}-{end_idx}: {e}")
                # Try to print the problematic rows for debugging
                print("First few rows in the problematic batch:")
                print(batch_df.head())
        # Create indexes after all data is inserted
        print("\n📊 Creating indexes...")
        con.execute("CREATE INDEX idx_date ON sales_data (date)")
        con.execute("CREATE INDEX idx_customer ON sales_data (customer_id)")
        con.execute("CREATE INDEX idx_product ON sales_data (product_id)")
        
        # Create additional indexes for analytics performance
        print("📊 Creating additional analytical indexes...")
        con.execute("CREATE INDEX idx_age ON sales_data (age)")
        con.execute("CREATE INDEX idx_income ON sales_data (income)")
        con.execute("CREATE INDEX idx_country ON sales_data (country)")
        con.execute("CREATE INDEX idx_city ON sales_data (city)")
        con.execute("CREATE INDEX idx_gender ON sales_data (gender)")
        con.execute("CREATE INDEX idx_transaction_type ON sales_data (transaction_desc)")
        
        # Add an index specifically for hour-based queries
        print("📅 Creating hour-based index...")
        con.execute("CREATE INDEX idx_hour ON sales_data (EXTRACT(hour FROM date))")
        
        # Get statistics about the table
        print("\n📈 Database statistics:")
        record_count = con.execute("SELECT COUNT(*) from sales_data").fetchone()[0]
        revenue = con.execute("SELECT SUM(total_amount_per_product_sgd) from sales_data").fetchone()[0]
        unique_customers = con.execute("SELECT COUNT(DISTINCT customer_id) from sales_data").fetchone()[0]
        
        
        print("\n✅ Data saved to sales_timeseries.db database file")
        print(f"📊 Total records: {record_count:,}")
        print(f"💰 Total revenue: SGD ${revenue:,.2f}")
        print(f"👥 Unique customers: {unique_customers:,}")
       
        print("🎉 Database creation complete!")                 

def generate_initial_data2(start_iteration:str, end_iteration:str, save_to_duckdb=True, db_path=SALES_TIMESERIES_DB):
    print("🏪 Retail Sales Database Generator")
    print("=" * 40)

    random.seed(142)

    # Create a date range with hourly frequency
    start_date = datetime.fromisoformat(start_iteration.replace(' ', 'T'))
    end_date = datetime.fromisoformat(end_iteration.replace(' ', 'T'))
    
    # Direct DuckDB generation - no parquet files needed

    # Calculate full date range first to get total days
    # Generate hourly range manually
    full_range = []
    current_date = start_date
    while current_date <= end_date:
        full_range.append(current_date)
        current_date += timedelta(hours=1)
    

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
        'customer_id': 'customer_id',
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

    # Initialize batch processing variables for direct DuckDB insertion
    all_transactions = []
    total_transactions = 0
    
    print(f"⚡ Processing {len(full_range)} time periods with memory-efficient batching...")
    
    # Monitor memory usage during generation
    progress_bar = tqdm(full_range, desc="Generating data")
    
    for date_idx, date in enumerate(progress_bar):
        # Check memory usage periodically
        if date_idx % 100 == 0:  # Check every 100 iterations
            current_memory = get_memory_usage()
            if current_memory > 0:
                progress_bar.set_postfix(memory=f"{current_memory:.0f}MB")
                
                # Warning if memory usage is getting high (over 1GB)
                if current_memory > 1024:
                    print(f"\n⚠️ High memory usage detected: {current_memory:.0f}MB")
                    print("Consider reducing date range or stopping generation")
        
        #build age
        age_ranges = [18, 25, 35, 45, 55, 65, 75]
        age_weights = [0.05, 0.20, 0.25, 0.25, 0.15, 0.08, 0.02]
        age_range_start = random.choices(age_ranges, weights=age_weights)[0]
        age = random.randint(age_range_start, min(age_range_start + 10, 80))
        
        #build city
        city = random.choice([city['name'] for city in cities])
        selected_city = next(item for item in cities if item['name'] == city)
        
        #build country 
        country_id = selected_city['country_id']
        country = selected_city['country']
        
        transaction_type = transaction_types[random.choices([0,1,2], weights=[0.95,0.025,0.025])[0]]
        
        #build income
        incomes =[30_000,40_000,50_000,60_000,70_000,80_000,90_000,100_000,120_000,150_000,200_000]
        income = int(random.choice(range(min(incomes), max(incomes))))
        
        
        #build product 
        product_id =1
        product = random.choice([p['product_name'] for p in products])
        
        units_sold = random.randint(1, 13)  # 1-13 units per item
        unit_price = random.choice([2345,3398,1234,2234,678,890,456,234,567,890,345,1234,5678,2345,3787])
        #unit_price =  np.random.choice([p['unit_price'] for p in products])    
        total_amount_per_product = units_sold * unit_price
        receipt_total = 0
                    
                    # Add hour variation throughout the day
        hour = random.randint(6, 21)  # Store hours 6 AM to 10 PM
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        # Generate fewer transactions per time period to reduce memory usage
        num_transactions = random.randint(1, MAX_TRANSACTIONS_PER_HOUR)
        
        for tx_num in range(num_transactions):
            # Create transaction with some time variation
            hour_offset = random.randint(0, 23)
            minute_offset = random.randint(0, 59)
            transaction_datetime = date + timedelta(hours=hour_offset, minutes=minute_offset, seconds=second)
            transaction_id = random.randint(1000000, 9999999)
            
            transaction_record = {
                'date': transaction_datetime,
                'transaction_id': transaction_id,
                'transaction_desc': transaction_type,
                'customer_id': str(customer_id),
                'age': age,
                'gender': random.choice(genders),
                'receipt_number': f'{transaction_id+receipt_id}',
                'product_id': product_id,
                'product_name': product,
                'units_sold': units_sold,
                'unit_price_sgd': round(unit_price, 2),
                'total_amount_per_product_sgd': round(total_amount_per_product, 2),
                'receipt_total_sgd': 0,
                'country_id': country_id,
                'country': country,
                'city': city,
                'income': income
            }
            
            all_transactions.append(transaction_record)
            total_transactions += 1
            
            customer_id += 1
            receipt_id += 1
            transaction_id += 1
    
    print(f"✅ Generated {total_transactions:,} transactions for direct DuckDB insertion")
    
    # Save directly to DuckDB if requested
    if save_to_duckdb and all_transactions:
        print(f"💾 Saving {len(all_transactions):,} transactions directly to DuckDB...")
        
        try:
            # Convert to DuckDB relation in temporary connection
            temp_con = duckdb.connect()
            
            # Create relation from dictionary list
            if all_transactions:
                # Get column names from first record
                columns = list(all_transactions[0].keys())
                
                # Create table schema
                temp_con.execute(f"""
                    CREATE TABLE chunk_data (
                        {', '.join([f"{col} VARCHAR" for col in columns])}
                    )
                """)
                
                # Insert data row by row
                for row in all_transactions:
                    values = [str(row.get(col, '')) for col in columns]
                    placeholders = ', '.join(['?' for _ in columns])
                    temp_con.execute(f"INSERT INTO chunk_data VALUES ({placeholders})", values)
            
            # Save to persistent database
            save_to_duckdb_table(temp_con, 'chunk_data', db_path)
            
            # Close temporary connection
            temp_con.close()
            
            return {
                'total_transactions': len(all_transactions),
                'start_date': str(start_date),
                'end_date': str(end_date),
                'saved_to_db': True
            }
        except Exception as e:
            print(f"❌ Error in generate_initial_data2: {e}")
            return all_transactions
    else:
        # Return transaction data for memory processing
        return all_transactions
            
            # After every 10 days, save to database to avoid memory issues                    

def initialize_database():
    # After all chunks, process and insert into database
    print("\n💾 Initializing database...")
    # We'll create the table directly in save_to_duckdb function
    # This function is kept for compatibility but doesn't need to create the table
   


def legacy_parquet_function_removed():
    """
    Parquet functionality has been removed - use direct DuckDB generation instead
    """
    print("❌ Parquet functionality has been removed.")
    print("💡 Use option 1 to generate data directly to DuckDB database.")
    return False

def load_dataset(mydf) -> None:
    """Display dataset from memory connection or DuckDB relation"""
    # If there is dataset in memory, display it as a table
    if mydf is not None:
        print("📊 Displaying Dataset from Memory")
        print("=" * 40)
        try:
            # Check if it's a DuckDB connection or relation
            if hasattr(mydf, 'execute'):
                # It's a connection, check for dataset table
                tables = mydf.execute("SHOW TABLES").fetchall()
                table_name = 'dataset'
                if not any('dataset' in str(table) for table in tables):
                    # Try sales_data table
                    if any('sales_data' in str(table) for table in tables):
                        table_name = 'sales_data'
                    else:
                        print("No dataset or sales_data table found in memory.")
                        return mydf
                
                # Display first 10 rows in table format
                sample_results = mydf.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
                columns = [desc[1] for desc in mydf.execute(f"PRAGMA table_info('{table_name}')").fetchall()]
                
                # Print header
                print(" | ".join(f"{col[:15]:<15}" for col in columns))
                print("-" * (len(columns) * 17))
                
                # Print rows
                for row in sample_results:
                    print(" | ".join(f"{str(val)[:15]:<15}" for val in row))
                
                # Show total count
                row_count = mydf.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"\nTotal rows in memory: {row_count:,}")
                
            elif hasattr(mydf, 'df'):
                # It's a DuckDB relation
                df_sample = mydf.limit(10).df()
                print(df_sample)
                print(f"\nTotal rows: {len(mydf.df()):,}")
                
        except Exception as e:
            print(f"Error displaying dataset: {e}")
        
        return mydf
     
    # If no data in memory, suggest loading
    print("❌ No dataset in memory.")
    print("💡 Use option 1 to generate data or option 3 to load from parquet files.")
    return None
   
    

def load_dataset_from_duckdb(db_path=SALES_TIMESERIES_DB) -> duckdb.DuckDBPyConnection:
    print("🔄 Loading dataset from DuckDB database...")
    try:
        # Use a fresh connection that we can return for memory operations
        con = duckdb.connect()
        
        # Check if source database exists
        import os
        if not os.path.exists(db_path):
            print(f"❌ Database file {db_path} not found.")
            return None
            
        # Load data from the persistent database into memory connection using ATTACH
        con.execute(f"ATTACH '{db_path}' AS source_db (READ_ONLY)")
        
        # Check if sales_data table exists using DuckDB's PRAGMA
        try:
            # Try to access the table directly - if it doesn't exist, this will fail
            test_result = con.execute("SELECT COUNT(*) FROM source_db.sales_data LIMIT 1").fetchone()
            print(f"📊 Found sales_data table with data in database")
        except Exception as table_error:
            print("❌ No sales_data table found in database.")
            print(f"   Error: {table_error}")
            con.execute("DETACH source_db")
            return None
            
        # Create dataset table from sales_data
        con.execute("CREATE TABLE dataset AS SELECT * FROM source_db.sales_data")
        con.execute("DETACH source_db")
        
        # Verify data was loaded
        row_count = con.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
        print(f"✅ Dataset loaded from DuckDB into memory: {row_count:,} rows")
        return con
        
    except Exception as e:
        print(f"❌ Error loading dataset from DuckDB: {e}")
        # If database doesn't exist or has no data, return empty connection
        try:
            con = duckdb.connect()
            con.execute("CREATE TABLE dataset AS SELECT 1 as dummy WHERE 1=0")  # Empty table with no rows
            return con
        except:
            return None

def clear_database_locks():
    """Force clear any database locks by ensuring all connections are closed"""
    import gc
    gc.collect()  # Force garbage collection to clean up any unclosed connections
    

def split_hourly_range(start_datetime, end_datetime, num_parts=None):
    # Calculate total hours first to avoid generating large lists
    total_hours = int((end_datetime - start_datetime).total_seconds() / 3600) + 1
    
    # Limit processing to prevent memory issues
    if total_hours > 8760:  # More than a year
        print(f"⚠️ Large date range detected: {total_hours} hours")
        print("Consider using smaller date ranges for better performance")
    
    # Generate smaller chunks based on hours rather than all timestamps
    hours_per_chunk = min(MAX_HOURS_PER_CHUNK, max(1, total_hours // 10))  # At least 10 chunks
    num_parts = max(1, (total_hours + hours_per_chunk - 1) // hours_per_chunk)
    
    print(f"⚙️ Total time span: {total_hours} hours, using {num_parts} chunks of ~{hours_per_chunk} hours each")
    
    ranges = []
    current_start = start_datetime
    
    for i in range(num_parts):
        # Calculate end time for this chunk
        hours_in_chunk = min(hours_per_chunk, total_hours - (i * hours_per_chunk))
        chunk_end = current_start + timedelta(hours=hours_in_chunk - 1)
        
        # Make sure we don't exceed the end date
        if chunk_end > end_datetime:
            chunk_end = end_datetime
            
        ranges.append((current_start, chunk_end))
        current_start = chunk_end + timedelta(hours=1)
        
        if current_start > end_datetime:
            break
    
    return ranges
 
    
def display_database_indexes(db_path=SALES_TIMESERIES_DB):
    """Display all indexed fields in the database"""
    print("\n📊 Database Indexes")
    print("=" * 40)
    
    # Check if the database file exists before connecting
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        print(f"Expected at: {os.path.abspath(db_path)}")
        return
        
    # Connect to the database
    try:
        with duckdb.connect(db_path, read_only=True) as con:
                
            # Get the table schema to see all columns
            print("📋 Database Schema:")
            schema = con.execute("DESCRIBE sales_data").fetchall()
            print(f"Found {len(schema)} columns in the 'sales_data' table:")
            for idx, row in enumerate(schema):
                # Handle different formats of DESCRIBE output (may have 2 or more columns)
                if len(row) >= 2:
                    col_name = row[0]
                    col_type = row[1]
                    print(f"  {idx+1}. {col_name} ({col_type})")
                else:
                    print(f"  {idx+1}. {row}")
            
            # List the indexes we've created
            print("\n🔑 Indexed Fields:")
            print("  1. Date (idx_date)")
            print("    - Purpose: Optimize date-based filtering and time series analysis")
            print("    - Example: SELECT * FROM sales_data WHERE date = '[first_date]'")
            
            print("  2. Customer ID (idx_customer)")
            print("    - Purpose: Speed up customer-specific queries")
            print("    - Example: SELECT * FROM sales_data WHERE customer_id = '100001'")
            
            print("  3. Product ID (idx_product)")
            print("    - Purpose: Enhance product-based filtering")
            print("    - Example: SELECT * FROM sales_data WHERE product_id = '100'")
            
            print("  4. Age (idx_age)")
            print("    - Purpose: Improve demographic analysis")
            print("    - Example: SELECT * FROM sales_data WHERE age BETWEEN 25 AND 40")
            
            print("  5. Income (idx_income)")
            print("    - Purpose: Speed up income-based segmentation")
            print("    - Example: SELECT * FROM sales_data WHERE income > 50000")
            
            print("  6. Country (idx_country)")
            print("    - Purpose: Optimize geographical queries")
            print("    - Example: SELECT * FROM sales_data WHERE country = 'Singapore'")
            
            print("  7. City (idx_city)")
            print("    - Purpose: Enhance location-based analysis")
            print("    - Example: SELECT * FROM sales_data WHERE city = 'New York'")
            
            print("  8. Gender (idx_gender)")
            print("    - Purpose: Speed up gender-based filtering")
            print("    - Example: SELECT * FROM sales_data WHERE gender = 'F'")
            
            print("  9. Transaction Type (idx_transaction_type)")
            print("    - Purpose: Improve transaction type filtering")
            print("    - Example: SELECT * FROM sales_data WHERE transaction_desc = 'Product Sale'")
            
            print("  10. Hour (idx_hour)")
            print("    - Purpose: Optimize hour-of-day analysis")
            print("    - Example: SELECT * FROM sales_data WHERE EXTRACT(hour FROM date) = 12")
            
            # Explain how indexes improve query performance
            print("\n📈 Benefits of Database Indexes:")
            print("  • Faster query execution for filtered and aggregated data")
            print("  • Improved performance for JOIN operations")
            print("  • More efficient sorting and grouping")
            print("  • Better time series analysis capabilities")
            while True:
                #ask 1-10 and run indexed query
                choice = input("\nEnter an index number (1-10) to run a sample query, or 'q' to quit: ")
                if choice.lower() == 'q':
                    break
                try:
                    index_num = int(choice)
                    if index_num < 1 or index_num > 10:
                        print("❌ Invalid choice. Please enter a number between 1 and 10.")
                        continue
                    
                    # Get dynamic values from the actual dataset
                    try:
                        first_date = con.execute("SELECT MIN(DATE(date)) FROM sales_data").fetchone()[0]
                        first_customer = con.execute("SELECT customer_id FROM sales_data LIMIT 1").fetchone()[0]
                        first_product = con.execute("SELECT product_id FROM sales_data LIMIT 1").fetchone()[0]
                        first_country = con.execute("SELECT country FROM sales_data WHERE country IS NOT NULL LIMIT 1").fetchone()[0]
                        first_city = con.execute("SELECT city FROM sales_data WHERE city IS NOT NULL LIMIT 1").fetchone()[0]
                        first_transaction = con.execute("SELECT transaction_desc FROM sales_data LIMIT 1").fetchone()[0]
                    except Exception as e:
                        print(f"⚠️ Could not get sample data values: {e}")
                        first_date = '2025-01-01'
                        first_customer = '100001'
                        first_product = '100'
                        first_country = 'Singapore'
                        first_city = 'New York'
                        first_transaction = 'Product Sale'
                    
                    # Map index number to sample queries using actual data values
                    sample_queries = {
                        1: f"SELECT COUNT(*) FROM sales_data WHERE DATE(date) = '{first_date}'",
                        2: f"SELECT COUNT(*) FROM sales_data WHERE customer_id = '{first_customer}'",
                        3: f"SELECT COUNT(*) FROM sales_data WHERE product_id = '{first_product}'",
                        4: "SELECT COUNT(*) FROM sales_data WHERE age BETWEEN 25 AND 40",
                        5: "SELECT COUNT(*) FROM sales_data WHERE income > 50000",
                        6: f"SELECT COUNT(*) FROM sales_data WHERE country = '{first_country}'",
                        7: f"SELECT COUNT(*) FROM sales_data WHERE city = '{first_city}'",
                        8: "SELECT COUNT(*) FROM sales_data WHERE gender = 'F'",
                        9: f"SELECT COUNT(*) FROM sales_data WHERE transaction_desc = '{first_transaction}'",
                        10: "SELECT COUNT(*) FROM sales_data WHERE EXTRACT(hour FROM date) = 12"
                    }

                    # Run the selected sample query
                    query = sample_queries[index_num]
                    print(f"\n🔍 Running query: {query}")
                    result = con.execute(query).fetchall()
                    for row in result:
                        print(f"  {row[0]}")
                except Exception as e:
                    print(f"❌ Error running query: {e}")

                
    except Exception as e:
        print(f"❌ Error accessing database: {str(e)}")

def display_field_descriptions(db_path=SALES_TIMESERIES_DB):
    """Display column descriptions for table sales_data (name, type, nullability, default)."""
    print("\n📋 Field Descriptions: sales_data")
    print("=" * 40)
    
    # Check if the database file exists before connecting
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        print(f"Expected at: {os.path.abspath(db_path)}")
        return
        
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            try:
                # Prefer PRAGMA table_info for detailed info
                rows = con.execute("PRAGMA table_info('sales_data')").fetchall()
                if not rows:
                    print("ℹ️ Table 'sales_data' not found or has no columns.")
                    return
                # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
                print("Columns:")
                for r in rows:
                    # Guard against varying tuple lengths
                    cid = r[0] if len(r) > 0 else None
                    name = r[1] if len(r) > 1 else "?"
                    coltype = r[2] if len(r) > 2 else "?"
                    notnull = r[3] if len(r) > 3 else 0
                    dflt = r[4] if len(r) > 4 else None
                    pk = r[5] if len(r) > 5 else 0
                    nn = "NOT NULL" if notnull else "NULLABLE"
                    pk_flag = " PK" if pk else ""
                    dflt_str = f" DEFAULT {dflt}" if dflt is not None else ""
                    print(f"  - {name} ({coltype}) {nn}{pk_flag}{dflt_str}")
            except Exception as inner:
                print(f"❌ Error reading table info: {inner}")
    except Exception as e:
        print(f"❌ Error accessing database: {e}")

def display_db_views(db_path=SALES_TIMESERIES_DB):
    """Display and run analytics views from the submenu"""
    
    # Define useful analytical views
    analytics_views = [
        {
            "name": "Monthly Sales Trends",
            "description": "Shows revenue and customer trends by month with growth percentages",
            "sql": """
                WITH monthly_sales AS (
                    SELECT 
                        DATE_TRUNC('month', date) AS month,
                        SUM(total_amount_per_product_sgd) AS revenue,
                        COUNT(DISTINCT customer_id) AS customers
                    FROM sales_data
                    GROUP BY DATE_TRUNC('month', date)
                    ORDER BY month
                ),
                with_prev AS (
                    SELECT 
                        month, 
                        revenue,
                        customers,
                        LAG(revenue) OVER (ORDER BY month) AS prev_revenue,
                        LAG(customers) OVER (ORDER BY month) AS prev_customers
                    FROM monthly_sales
                )
                SELECT 
                    month,
                    revenue,
                    customers,
                    prev_revenue,
                    prev_customers,
                    CASE 
                        WHEN prev_revenue IS NULL THEN NULL
                        WHEN prev_revenue = 0 THEN 
                            CASE 
                                WHEN revenue = 0 THEN 0
                                ELSE 100 -- from zero to something is 100% growth
                            END
                        ELSE ((revenue - prev_revenue) / prev_revenue) * 100 
                    END AS revenue_change_pct,
                    CASE
                        WHEN prev_customers IS NULL THEN NULL
                        WHEN prev_customers = 0 THEN 
                            CASE
                                WHEN customers = 0 THEN 0
                                ELSE 100
                            END
                        ELSE ((customers - prev_customers) / prev_customers) * 100
                    END AS customer_change_pct
                FROM with_prev
                ORDER BY month
            """
        },
        {
            "name": "Customer Demographics Analysis",
            "description": "Segments customers by age groups and gender with spending patterns",
            "sql": """
                SELECT 
                    CASE 
                        WHEN age BETWEEN 18 AND 25 THEN '18-25'
                        WHEN age BETWEEN 26 AND 35 THEN '26-35'
                        WHEN age BETWEEN 36 AND 45 THEN '36-45'
                        WHEN age BETWEEN 46 AND 55 THEN '46-55'
                        WHEN age BETWEEN 56 AND 65 THEN '56-65'
                        WHEN age > 65 THEN '65+'
                        ELSE 'Unknown'
                    END AS age_group,
                    gender,
                    COUNT(DISTINCT customer_id) AS customer_count,
                    AVG(income) AS avg_income,
                    SUM(total_amount_per_product_sgd) AS total_spent,
                    SUM(total_amount_per_product_sgd) / COUNT(DISTINCT customer_id) AS avg_spent_per_customer
                FROM sales_data
                GROUP BY age_group, gender
                ORDER BY age_group, gender
            """
        },
        {
            "name": "Top Products by Revenue",
            "description": "Shows the best-selling products with sales metrics",
            "sql": """
                SELECT 
                    product_id,
                    product_name,
                    COUNT(*) AS transaction_count,
                    SUM(units_sold) AS total_units_sold,
                    SUM(total_amount_per_product_sgd) AS total_revenue,
                    AVG(unit_price_sgd) AS avg_price
                FROM sales_data
                GROUP BY product_id, product_name
                ORDER BY total_revenue DESC
                LIMIT 20
            """
        },
        {
            "name": "Hourly Sales Distribution",
            "description": "Analyzes sales patterns by hour of day",
            "sql": """
                SELECT 
                    EXTRACT(hour FROM date) AS hour_of_day,
                    COUNT(*) AS transaction_count,
                    SUM(total_amount_per_product_sgd) AS total_revenue,
                    COUNT(DISTINCT customer_id) AS unique_customers
                FROM sales_data
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """
        },
        {
            "name": "Geographic Sales Distribution",
            "description": "Shows sales by country and city with rankings",
            "sql": """
                WITH country_sales AS (
                    SELECT 
                        country,
                        COUNT(*) AS transactions,
                        COUNT(DISTINCT customer_id) AS customers,
                        SUM(total_amount_per_product_sgd) AS total_revenue
                    FROM sales_data
                    GROUP BY country
                ),
                ranked_countries AS (
                    SELECT 
                        country,
                        transactions,
                        customers,
                        total_revenue,
                        RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank
                    FROM country_sales
                )
                SELECT
                    country,
                    transactions,
                    customers,
                    total_revenue,
                    revenue_rank
                FROM ranked_countries
                ORDER BY revenue_rank
                LIMIT 15
            """
        },
        {
            "name": "High-Value vs Low-Value Transactions",
            "description": "Compares highest and lowest value transactions by various dimensions",
            "sql": """
                WITH transaction_values AS (
                    SELECT
                        date,
                        transaction_id,
                        customer_id,
                        age,
                        gender,
                        country,
                        city,
                        total_amount_per_product_sgd,
                        NTILE(10) OVER (ORDER BY total_amount_per_product_sgd) AS value_decile
                    FROM sales_data
                )
                SELECT
                    CASE 
                        WHEN value_decile = 1 THEN 'Bottom 10%'
                        WHEN value_decile = 10 THEN 'Top 10%'
                    END AS value_segment,
                    COUNT(*) AS transaction_count,
                    COUNT(DISTINCT customer_id) AS unique_customers,
                    AVG(age) AS avg_customer_age,
                    AVG(total_amount_per_product_sgd) AS avg_transaction_value,
                    SUM(total_amount_per_product_sgd) AS total_revenue,
                    (SUM(total_amount_per_product_sgd) / COUNT(*)) / 
                        (SELECT AVG(total_amount_per_product_sgd) FROM sales_data) AS relative_to_average
                FROM transaction_values
                WHERE value_decile IN (1, 10)
                GROUP BY value_segment
                ORDER BY value_segment
            """
        },
        {
            "name": "Income Level Analysis",
            "description": "Segments customers by income levels with purchasing patterns",
            "sql": """
                WITH income_segments AS (
                    SELECT
                        CASE
                            WHEN income < 30000 THEN 'Low Income (< 30k)'
                            WHEN income BETWEEN 30000 AND 60000 THEN 'Middle Income (30k-60k)'
                            WHEN income BETWEEN 60001 AND 100000 THEN 'Upper Middle (60k-100k)'
                            WHEN income > 100000 THEN 'High Income (>100k)'
                            ELSE 'Unknown'
                        END AS income_segment,
                        customer_id,
                        total_amount_per_product_sgd
                    FROM sales_data
                )
                SELECT
                    income_segment,
                    COUNT(DISTINCT customer_id) AS customer_count,
                    COUNT(*) AS transaction_count,
                    SUM(total_amount_per_product_sgd) AS total_revenue,
                    AVG(total_amount_per_product_sgd) AS avg_transaction_value,
                    SUM(total_amount_per_product_sgd) / COUNT(DISTINCT customer_id) AS avg_spent_per_customer
                FROM income_segments
                GROUP BY income_segment
                ORDER BY 
                    CASE 
                        WHEN income_segment = 'Low Income (< 30k)' THEN 1
                        WHEN income_segment = 'Middle Income (30k-60k)' THEN 2
                        WHEN income_segment = 'Upper Middle (60k-100k)' THEN 3
                        WHEN income_segment = 'High Income (>100k)' THEN 4
                        ELSE 5
                    END
            """
        }
    ]
    
    while True:
        # Display views menu
        print("\n🪟 Analytics Views")
        print("=" * 40)
        
        # Check if the database file exists before connecting
        if not os.path.exists(db_path):
            print(f"❌ Database file not found: {db_path}")
            print(f"Expected at: {os.path.abspath(db_path)}")
            return
            
        num_views = len(analytics_views)
        print(f"Select a view to run (1-{num_views} or 0 to return):")
        
        # Display view options
        for i, view in enumerate(analytics_views, 1):
            print(f"  {i}. {view['name']}")
            print(f"     📊 {view['description']}")
            
        print("  0. Return to main menu")
        
        # Get user choice
        try:
            choice = input(f"\nSelect view (0-{len(analytics_views)}): ").strip()
            
            if choice == '0':
                return
                
            try:
                view_idx = int(choice) - 1
                if 0 <= view_idx < len(analytics_views):
                    selected_view = analytics_views[view_idx]
                else:
                    print(f"❌ Invalid choice. Please enter a number between 0 and {len(analytics_views)}.")
                    continue
            except ValueError:
                print(f"❌ Invalid input. Please enter a number between 0 and {len(analytics_views)}.")
                continue
                
            # Execute the selected view
            print(f"\n📈 Running: {selected_view['name']}")
            print("=" * 40)
            print(f"📝 {selected_view['description']}")
            
            try:
                with duckdb.connect(db_path, read_only=True) as con:
                    # Run the SQL query and get results without DataFrame
                    query_result = con.execute(selected_view['sql'])
                    columns = [desc[0] for desc in query_result.description] if hasattr(query_result, 'description') else []
                    rows = query_result.fetchall()
                    
                    if not rows:
                        print("\n❌ No results returned.")
                    else:
                        # Print results without DataFrame
                        print(f"\nResults: {len(rows)} rows")
                        print("-" * 40)
                        
                        # Print header
                        if columns:
                            print(" | ".join(f"{col[:15]:<15}" for col in columns))
                            print("-" * (len(columns) * 17))
                        
                        # Print rows (limit to 20 for readability)
                        for i, row in enumerate(rows[:20]):
                            print(" | ".join(f"{str(val)[:15]:<15}" for val in row))
                            
                        if len(rows) > 20:
                            print(f"... and {len(rows) - 20} more rows")
                        
                        # Ask if user wants to save results
                        save_option = input("\nSave results to CSV? (y/n): ").strip().lower()
                        if save_option == 'y' and rows:
                            # Create a valid filename from the view name
                            view_name_for_file = ''.join(c if c.isalnum() else '_' for c in selected_view['name'])
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            csv_filename = f"{view_name_for_file}_{timestamp}.csv"
                            
                            # Save to workspace directory using DuckDB
                            save_path = os.path.join(os.path.dirname(db_path), csv_filename)
                            con.execute(f"COPY ({selected_view['sql']}) TO '{save_path}' (FORMAT CSV, HEADER)")
                            print(f"✅ Results saved to: {save_path}")
                        
                        # Offer insights based on the view
                        print("\n💡 Key Insights:")
                        
                        if "Monthly Sales Trends" in selected_view['name'] and rows:
                            # Simple insights without DataFrame operations
                            print(f"  • Analysis shows {len(rows)} time periods")
                            if len(rows) > 0:
                                print(f"  • Data spans multiple months with trend analysis")
                                print(f"  • See detailed results above for growth patterns")
                            else:
                                print("  • No revenue data available to analyze trends")
                            
                        elif "Demographics" in selected_view['name'] and rows:
                            print(f"  • Demographics analysis shows {len(rows)} customer segments")
                            if rows:
                                print(f"  • See results above for spending patterns by age and gender")
                        
                        elif "Top Products" in selected_view['name'] and rows:
                            if rows:
                                print(f"  • Analysis shows {len(rows)} product performance metrics")
                                print(f"  • Top product details shown in results above")
                        
                        elif "Hourly Sales" in selected_view['name'] and rows:
                            if rows:
                                print(f"  • Hourly analysis covers {len(rows)} hour periods")
                                print(f"  • Peak hours and transaction patterns shown above")
                        
                        elif "Geographic" in selected_view['name'] and rows:
                            if rows:
                                print(f"  • Geographic analysis shows {len(rows)} markets")
                                print(f"  • Market performance details shown above")
                        
                        try:
                            input("\nPress Enter to continue...")
                        except EOFError:
                            return
                        
            except Exception as e:
                print(f"❌ Error running view: {str(e)}")
                try:
                    input("\nPress Enter to continue...")
                except EOFError:
                    return
        except EOFError:
            return
        except ValueError:
            print("❌ Please enter a valid number.")
                
    # Display options no longer needed

def display_db_saved_queries(db_path=SALES_TIMESERIES_DB):
    """Display saved queries/macros stored in the database (DuckDB macros)."""
    print("\n📝 Saved Queries / Macros")
    print("=" * 40)
    
    # Check if the database file exists before connecting
    if not os.path.exists(db_path):
        print(f"❌ Database file not found: {db_path}")
        print(f"Expected at: {os.path.abspath(db_path)}")
        return
    
    has_found_items = False
        
    try:
        with duckdb.connect(db_path, read_only=True) as con:
            # Prefer duckdb_macros() which includes SQL definitions
            try:
                # Try quietly without printing debug info
                macros = con.execute("SELECT schema_name, macro_name, macro_type, parameters, macro_sql FROM duckdb_macros() ORDER BY schema_name, macro_name").fetchall()
                if macros:
                    has_found_items = True
                    print("📚 Saved macros:")
                    for schema_name, macro_name, macro_type, params, macro_sql in macros:
                        print(f"  - {schema_name}.{macro_name} [{macro_type}]")
                        if params:
                            print(f"      Params: {params}")
                        if macro_sql:
                            first_line = " ".join(str(macro_sql).split())
                            print(f"      SQL: {first_line}")
                # Return only if we found items
                if has_found_items:
                    return
            except Exception:
                # Silently continue to next method
                pass
            try:
                rows = con.execute("SHOW MACROS").fetchall()
                if rows:
                    has_found_items = True
                    print("📜 Stored macros:")
                    for r in rows:
                        # SHOW MACROS returns at least a name; print generically
                        print(f"  - {', '.join(str(x) for x in r if x is not None)}")
            except Exception:
                # Silently continue
                pass
                
            # Try alternate approach - look for prepared statements
            try:
                # Try silently
                prep_statements = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%prepared%'").fetchall()
                if prep_statements:
                    has_found_items = True
                    print("📋 Prepared statements:")
                    for stmt in prep_statements:
                        print(f"  - {stmt[0] if stmt else 'Unknown'}")
            except Exception:
                # Silently continue
                pass
                
            # Try to find views as they're also a form of saved query
            try:
                views = con.execute("SELECT table_name FROM information_schema.views").fetchall()
                if views:
                    has_found_items = True
                    print("🔍 Views (saved queries):")
                    for view in views:
                        print(f"  - {view[0] if view else 'Unknown'}")
            except Exception:
                # Silently continue
                pass
                
            # If nothing was found in any method, show a friendly message
            if not has_found_items:
                print("📭 No saved queries or macros were found in the database.")
                print("\nℹ️ To create saved queries in DuckDB, you can:")
                print("  1. Create views using CREATE VIEW")
                print("  2. Use prepared statements")
                print("  3. Create table-valued functions")
                
            # Pause: "Continue to iterate?"
            input("\nContinue to iterate? (Press Enter to continue)")
    except Exception as e:
        print(f"❌ Error accessing database: {e}")

def clear_database():
    """Clear/reset the DuckDB database by deleting all data"""
    import os
    
    try:
        if os.path.exists(SALES_TIMESERIES_DB):
            # Confirm with user before deleting
            print(f"🗑️ This will delete all data in: {SALES_TIMESERIES_DB}")
            confirm = input("Are you sure you want to delete all data? (y/N): ").strip().lower()
            
            if confirm == 'y' or confirm == 'yes':
                # Try to connect and drop the table first
                try:
                    conn = duckdb.connect(SALES_TIMESERIES_DB)
                    conn.execute("DROP TABLE IF EXISTS sales_data")
                    conn.close()
                    print("✅ Database table cleared successfully")
                except Exception as e:
                    print(f"⚠️ Could not clear table: {e}")
                    
                    # If table drop fails, delete the entire file
                    try:
                        os.remove(SALES_TIMESERIES_DB)
                        print("✅ Database file deleted successfully")
                    except Exception as e2:
                        print(f"❌ Could not delete database file: {e2}")
                        return False
            else:
                print("❌ Database clearing cancelled")
                return False
        else:
            print("📭 No database file found to clear")
            
        return True
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")
        return False

def clear_database_locks():
    """Force clear any database locks by ensuring all connections are closed"""
    import gc
    import os
    
    # Force garbage collection to clean up any unclosed connections
    gc.collect()
    
    # Check if database file exists and is accessible
    if os.path.exists(SALES_TIMESERIES_DB):
        try:
            # Try to briefly connect and disconnect to test accessibility
            test_con = duckdb.connect(database=SALES_TIMESERIES_DB, read_only=True)
            test_con.close()
        except Exception as e:
            print(f"⚠️ Database lock detected: {e}")
            print("💡 Tip: Restart the application if locks persist")
    
    print("🔓 Database locks cleared")


def read_generated_csv_files():
    """Option 9: Read CSV files generated by the system"""
    print("\n📄 CSV Files Generated by the System")
    print("=" * 35)
    
    # Look for CSV files in common locations
    csv_search_paths = [
        SOURCE_DIR,  # src directory
        OUTPUT_ROOT,  # main project directory
        os.path.join(SOURCE_DIR, 'exports'),  # potential exports folder
    ]
    
    found_csv_files = []
    
    # Search for CSV files
    for search_path in csv_search_paths:
        if os.path.exists(search_path):
            try:
                for file in os.listdir(search_path):
                    if file.endswith('.csv') and not file.startswith('.'):
                        full_path = os.path.join(search_path, file)
                        file_size = os.path.getsize(full_path)
                        file_mod_time = datetime.fromtimestamp(os.path.getmtime(full_path))
                        found_csv_files.append({
                            'name': file,
                            'path': full_path,
                            'size': file_size,
                            'modified': file_mod_time
                        })
            except Exception as e:
                print(f"⚠️ Could not search in {search_path}: {e}")
    
    if not found_csv_files:
        print("📭 No CSV files generated from option 8")
        print("\n💡 To generate CSV files:")
        print("   - Use option 8 (Display database views) if it has CSV export")
        print("   - Or use the Retail Menu (option 9 in older version) → Export to CSV")
    else:
        print(f"📊 Found {len(found_csv_files)} CSV file(s):")
        print()
        
        for i, csv_file in enumerate(found_csv_files, 1):
            size_mb = csv_file['size'] / 1024 / 1024
            print(f"  {i}. {csv_file['name']}")
            print(f"     📁 Path: {csv_file['path']}")
            print(f"     📏 Size: {size_mb:.2f} MB")
            print(f"     📅 Modified: {csv_file['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
            print()
        
        # Ask user which file to read
        try:
            choice = input(f"Select a file to preview (1-{len(found_csv_files)}) or 'q' to quit: ").strip()
            if choice.lower() != 'q':
                file_num = int(choice)
                if 1 <= file_num <= len(found_csv_files):
                    selected_file = found_csv_files[file_num - 1]
                    preview_csv_file(selected_file['path'])
                else:
                    print("❌ Invalid file number")
        except ValueError:
            print("❌ Invalid input")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    input("\nPress Enter to continue...")


def preview_csv_file(csv_path):
    """Preview a CSV file using DuckDB"""
    print(f"\n📋 Preview of: {os.path.basename(csv_path)}")
    print("=" * 50)
    
    try:
        # Use DuckDB to read CSV file
        con = duckdb.connect()
        
        # Get basic info
        row_count = con.execute(f"SELECT COUNT(*) FROM '{csv_path}'").fetchone()[0]
        print(f"📊 Total rows: {row_count:,}")
        
        # Show first 10 rows
        print(f"\n📄 First 10 rows:")
        print("-" * 50)
        
        result = con.execute(f"SELECT * FROM '{csv_path}' LIMIT 10").fetchall()
        columns = [desc[0] for desc in con.description]
        
        # Print header
        header = " | ".join(f"{col[:15]:<15}" for col in columns)
        print(header)
        print("-" * len(header))
        
        # Print rows
        for row in result:
            row_str = " | ".join(f"{str(val)[:15]:<15}" for val in row)
            print(row_str)
        
        con.close()
        
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        print("💡 Trying to read as plain text...")
        
        # Fallback to reading as text
        try:
            with open(csv_path, 'r') as f:
                lines = f.readlines()[:10]
                for i, line in enumerate(lines):
                    print(f"{i+1:2}: {line.rstrip()}")
        except Exception as e2:
            print(f"❌ Could not read file: {e2}")


def main():
    # Clear any existing database locks
    clear_database_locks()
    
    df_all = None  # Initialize df_all to avoid reference issues later
    print("🏪 Retail TimeSeries Database Generator")
    print("=" * 40)

    while True:
        print("\n🔄 Options:")
        print("   1️⃣  Generate sample dataset directly to DuckDB")
        print("    2. Load dataset from DuckDB to memory")
        print("    3. [REMOVED] Parquet functionality removed")
        print("    4. Display dataset on screen")
        print("    5. Display database statistics")
        print("    6. Display database indexes")
        print("    7. Display field descriptions") 
        print("    8. Display database views")
        print("    9. Read CSV files generated by the system")
        print("    C. Clear/Reset database (delete all data)")
        print("    L. Clear database locks")
        print("    0. Exit")

        choice = input("\nSelect option (0-9, C, L): ").strip().lower()

        if choice == '1':
            mydates = ask_parameters()
            is_initial_generation = True
            df_all = generate_initial_data1(mydates, is_initial_generation=is_initial_generation)
            #start = input("Enter start datetime (YYYY-MM-DD HH:MM:SS) [default: 1900-01-01 00:00:00]: ").strip() or '1900-01-01 00:00:00'
            #end = input("Enter end datetime (YYYY-MM-DD HH:MM:SS) [default: 2025-09-02 23:00:00]: ").strip() or '2025-09-02 23:00:00'
            #print("⚡ Generating new database...")
        
        elif choice == '2':
            print("🔄 Loading dataset from DuckDB database to memory...")
            df_all = load_dataset_from_duckdb()
        
        elif choice == '3':
            print('❌ Parquet functionality has been removed.')
            print('💡 Use option 1 to generate data directly to DuckDB database.')

        elif choice == '4':
            print("\n📊 Displaying Sample Dataset")
            print("=" * 40)
            if df_all is not None:
                # Show first 10 rows using DuckDB without creating DataFrame
                sample_results = df_all.execute("SELECT * FROM dataset LIMIT 10").fetchall()
                columns = [desc[1] for desc in df_all.execute("PRAGMA table_info('dataset')").fetchall()]
                
                # Print header
                print(" | ".join(f"{col[:15]:<15}" for col in columns))
                print("-" * (len(columns) * 17))
                
                # Print rows
                for row in sample_results:
                    print(" | ".join(f"{str(val)[:15]:<15}" for val in row))
                
                row_count = df_all.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
                print(f"\nTotal rows: {row_count:,}")
            else:
                print("No data loaded. Please load or generate dataset first.")
            
            
        
        
        
        
        
            
           # # Get total rows and show sample size
           # total_rows = len(df_all)
           # sample_rows = min(5, total_rows)  # Show at most 5 rows
           # print(f"Total records: {total_rows:,} | Showing first {sample_rows} records\n")
           # 
           # # Display the actual data
           # print(df_all.head(sample_rows).to_string(index=False))
           # print(f"\nDataset shape: {df_all.shape}")
           # print(f"Columns: {list(df_all.columns)}")
            
        elif choice == '5':
            # Display database statistics
            print("📊 Database Statistics")
            print("=" * 40)
            try:
                with duckdb.connect(database=SALES_TIMESERIES_DB, read_only=True) as con:
                    # Check if sales_data table exists
                    tables = con.execute("SHOW TABLES").fetchall()
                    if any('sales_data' in str(table) for table in tables):
                        # Show table stats
                        row_count = con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
                        print(f"📈 Total records: {row_count:,}")
                        
                        # Date range
                        date_stats = con.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM sales_data").fetchone()
                        print(f"📅 Date range: {date_stats[0]} to {date_stats[1]}")
                        
                        # Revenue stats
                        revenue_stats = con.execute("SELECT SUM(total_amount_per_product_sgd) as total_revenue, AVG(total_amount_per_product_sgd) as avg_revenue FROM sales_data").fetchone()
                        print(f"💰 Total revenue: ${revenue_stats[0]:,.2f}")
                        print(f"💰 Average transaction: ${revenue_stats[1]:,.2f}")
                        
                        # Customer stats
                        customer_count = con.execute("SELECT COUNT(DISTINCT customer_id) FROM sales_data").fetchone()[0]
                        print(f"👥 Unique customers: {customer_count:,}")
                        
                        # Product stats
                        product_count = con.execute("SELECT COUNT(DISTINCT product_id) FROM sales_data").fetchone()[0]
                        print(f"🛍️ Unique products: {product_count:,}")
                        
                    else:
                        print("❌ No sales_data table found. Generate data first with option 1.")
            except Exception as e:
                print(f"❌ Error reading database: {e}")
            
            # Group columns into logical sections
            column_groups = [
                ["date", "transaction_id", "transaction_desc", "customer_id"],
                ["age", "gender", "income", "country", "city"],
                ["receipt_number", "product_id", "product_name", "units_sold"],
                ["unit_price_sgd", "total_amount_per_product_sgd", "receipt_total_sgd", "country_id"]
            ]
            
            # Print column groups with index numbers and data types
            print("Column names grouped by category:")
            group_names = ["Date & Transaction Info", "Customer Demographics", "Product Details", "Financial & Location Codes"]
            
            for g_idx, (group_name, columns) in enumerate(zip(group_names, column_groups)):
                print(f"\nGroup {g_idx + 1}: {group_name}")
                print("-" * (len(group_name) + 10))
              # for col in columns:
              #     if col in df_all.columns:
              #         #duckdb data type not pandas
              #         i = df_all.columns[col] + 1  # 1-based index
              #         dtype = str(df_all[col].dtype)
              #         print(f"  {i}. {col:<25} [{dtype}]")
            
     #   # Format values for display
     #   def format_value(val):
     #       if pd.isna(val):
     #           return ""
     #       elif isinstance(val, (int, float)) and not isinstance(val, bool):
     #           if isinstance(val, float):
     #               return f"{val:,.2f}"
     #           return f"{val:,}"
     #       return str(val)
     #   
     #   # Print data in a multi-column layout
     #   print("\nData sample (Records with grouped fields):")
     #   print("=" * 40)
     #   
     #   for row_idx in range(5):
     #       if row_idx < len(df_all):
     #           row = df_all.iloc[row_idx]
     #           print(f"\n📄 Record #{row_idx + 1}:")
     #           print("-" * 40)
     #           
     #           for g_idx, (group_name, columns) in enumerate(zip(group_names, column_groups)):
     #               print(f"\n  {group_name}:")
     #               for col in columns:
     #                   if col in df_all.columns:
     #                       val = format_value(row[col])
     #                       print(f"    • {col:<25}: {val}")
     #               
     #           print("-" * 40)
                
         #      # Reset pandas display options to defaults
         #      pd.reset_option('display.max_columns')
         #      pd.reset_option('display.width')
         #      pd.reset_option('display.precision')
         #      pd.reset_option('display.max_colwidth')
         #      pd.reset_option('display.colheader_justify')
        elif choice == '6':
            display_database_indexes()
        elif choice == '7':
            display_field_descriptions()
        elif choice == '8':
            display_db_views()
        elif choice == '9':
            read_generated_csv_files()
        elif choice == 'c':
            print("🗑️ Clear/Reset Database")
            print("=" * 25)
            success = clear_database()
            if success:
                df_all = None  # Reset loaded data
                print("💡 You can now generate new data with option 1")
        elif choice == 'l':
            clear_database_locks()
            
        elif choice == '0':
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please select a valid option.")


if __name__ == "__main__":
    main()
    sys.exit(0)
