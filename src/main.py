

import json
import pandas as pd
import numpy as np
import duckdb
import pickle
from datetime import datetime, timedelta
from tqdm import tqdm
import os, time, glob
import sys
from pprint import pprint
from concurrent.futures import ProcessPoolExecutor, as_completed
from load_csv_to_df import load_csv_to_df
from io import StringIO


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

# Define standard chunk size for data processing (records per chunk)
CHUNK_SIZE = 6000

# Define output directories for split files
SPLIT_CSV_PATH = SPLIT_CSV_FILES  # Output CSV files in csvanalyzer/src/SPLITCSV
PARQUET_PATH = PARQUET_FILES  # Output Parquet files in csvanalyzer/src/PARQUET
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

def save_to_duckdb(df_all:pd.DataFrame, db_path:str=SALES_TIMESERIES_DB):
    print("📊 Saving data to DuckDB...")
    
    # Ensure all required columns exist
    required_columns = [
        'date', 'transaction_id', 'transaction_desc', 'customer_id', 
        'age', 'gender', 'receipt_number', 'product_id', 'product_name', 
        'units_sold', 'unit_price_sgd', 'total_amount_per_product_sgd', 
        'receipt_total_sgd', 'country_id', 'country', 'city', 'income'
    ]
    
    # Check for missing columns and add them if needed
    for col in required_columns:
        if col not in df_all.columns:
            print(f"⚠️ Adding missing column: {col}")
            df_all[col] = f"default_{col}"  # Add default value
    
    # Sanitize data types to prevent DuckDB errors
    print("🧹 Sanitizing data types...")
    
    # Convert string columns to ensure they are clean
    for col in df_all.select_dtypes(include=['object']).columns:
        df_all[col] = df_all[col].astype(str)
        # Remove any problematic characters
        df_all[col] = df_all[col].apply(lambda x: ''.join(c for c in x if c.isprintable()))
    
    # Convert numeric columns
    # Age should be numeric
    if 'age' in df_all.columns:
        df_all['age'] = pd.to_numeric(df_all['age'], errors='coerce').fillna(0).astype(int)
    
    if 'units_sold' in df_all.columns:
        df_all['units_sold'] = pd.to_numeric(df_all['units_sold'], errors='coerce').fillna(0).astype(int)
    
    # Reset all amount columns to zero as specified
    for col in ['unit_price_sgd', 'total_amount_per_product_sgd', 'receipt_total_sgd']:
        if col in df_all.columns:
            df_all[col] = 0.0
    
    # Income should be numeric
    if 'income' in df_all.columns:
        df_all['income'] = pd.to_numeric(df_all['income'], errors='coerce').fillna(0).astype(float)
    
    # Country fields as VARCHAR - already handled by the string conversion above
    
    # Ensure date is proper timestamp format
    if 'date' in df_all.columns:
        try:
            # Convert to datetime if it's not already
            if not pd.api.types.is_datetime64_any_dtype(df_all['date']):
                df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')
            
            # Replace NaT with a default date
            df_all['date'] = df_all['date'].fillna(pd.Timestamp('2000-01-01 00:00:00'))
        except Exception as e:
            print(f"⚠️ Error processing date column: {e}")
            # Use a default date if conversion fails
            df_all['date'] = pd.Timestamp('2000-01-01 00:00:00')
    
    # Only keep the required columns in the right order
    df_all = df_all[required_columns]
    
    # Debug information
    print(f"DataFrame columns: {df_all.columns.tolist()}")
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

def generate_initial_data(start_iteration:str,end_iteration:str, save_to_parquet=False):
    print("🏪 Retail Sales Database Generator")
    print("=" * 40)

    np.random.seed(142)

    # Create a date range with hourly frequency
    start_date = pd.to_datetime(start_iteration)
    end_date = pd.to_datetime(end_iteration)
    
    # Ensure Parquet directory exists
    os.makedirs(PARQUET_FILES, exist_ok=True)

    # Calculate full date range first to get total days
    full_range = pd.date_range(start=start_date, end=end_date, freq='H')
    

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
        for hour_of_day in range(0, 24):  # Simulate every hour of the day
            # Adjust the hour calculation to use the loop variable
            transaction_datetime = date + timedelta(hours=hour_of_day, minutes=minute, seconds=second)
            transaction_id = np.random.randint(1000000, 9999999)
            all_transactions.append({
                'date': transaction_datetime,
                'transaction_id': transaction_id,
                'transaction_desc': transaction_type,
                'customer_id': str(customer_id),  # Add missing customer_id column
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
    
    # Convert to DataFrame and save as Parquet file
    if save_to_parquet:
        print(f"Converting transactions to DataFrame...")
        df_all = pd.DataFrame(all_transactions)
        
        # Generate a unique filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parquet_filename = os.path.join(PARQUET_FILES, f'chunk_{timestamp}.parquet')
        
        print(f"Saving {len(df_all):,} rows to {parquet_filename}...")
        df_all.to_parquet(parquet_filename, index=False)
        print(f"✅ Saved data to {parquet_filename}")
        
        # Create metadata file
        metadata = {
            'filename': os.path.basename(parquet_filename),
            'rows': len(df_all),
            'start_date': str(start_date),
            'end_date': str(end_date),
            'created_at': timestamp
        }
        
        # Update or create metadata file
        metadata_path = os.path.join(PARQUET_FILES, 'chunks_metadata.json')
        
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r') as f:
                    existing_metadata = json.load(f)
                    if isinstance(existing_metadata, list):
                        existing_metadata.append(metadata)
                    else:
                        existing_metadata = [metadata]
            except (json.JSONDecodeError, FileNotFoundError):
                existing_metadata = [metadata]
        else:
            existing_metadata = [metadata]
            
        with open(metadata_path, 'w') as f:
            json.dump(existing_metadata, f, indent=2)
            
        return parquet_filename
    else:
        # If not saving to Parquet, return the list as before
        return all_transactions
            
            # After every 10 days, save to database to avoid memory issues                    

def initialize_database():
    # After all chunks, process and insert into database
    print("\n💾 Initializing database...")
    # We'll create the table directly in save_to_duckdb function
    # This function is kept for compatibility but doesn't need to create the table
   
    
def list_parquet_chunks(parquet_pattern=None):
    """
    List all available Parquet chunks and their metadata
    
    Args:
        parquet_pattern: Glob pattern for the parquet files to list
    """
    if parquet_pattern is None:
        parquet_pattern = os.path.join(PARQUET_FILES, 'batch_*.parquet')
        
    print("📋 Listing available Parquet chunks...")
    parquet_files = glob.glob(parquet_pattern)
    
    if not parquet_files:
        print("❌ No Parquet files found.")
        return
    
    total_size = 0
    total_rows = 0
    
    print(f"\n{'#':<4} {'Filename':<30} {'Size':<15} {'Rows':<10} {'Created':<20}")
    print("-" * 80)
    
    # Check if metadata file exists for more detailed information
    metadata_path = os.path.join(PARQUET_FILES, 'chunks_metadata.json')
    metadata = {}
    
    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r') as f:
                metadata_list = json.load(f)
                for item in metadata_list:
                    if 'filename' in item:
                        metadata[item['filename']] = item
        except (json.JSONDecodeError, FileNotFoundError):
            print("⚠️ Could not read metadata file.")
    
    # List each file with its information
    for i, file_path in enumerate(sorted(parquet_files)):
        filename = os.path.basename(file_path)
        size_bytes = os.path.getsize(file_path)
        size_mb = size_bytes / (1024 * 1024)
        
        # Try to get row count and other metadata if available
        rows = "Unknown"
        created = "Unknown"
        
        if filename in metadata:
            if 'rows' in metadata[filename]:
                rows = f"{metadata[filename]['rows']:,}"
                total_rows += metadata[filename]['rows']
            if 'created_at' in metadata[filename]:
                created = metadata[filename]['created_at']
        else:
            # If no metadata, try to read parquet file for row count
            try:
                # Just read the metadata without loading all data
                parquet_metadata = pd.read_parquet(file_path, columns=[])
                row_count = len(parquet_metadata)
                rows = f"{row_count:,}"
                total_rows += row_count
            except:
                pass
                
        total_size += size_bytes
        print(f"{i+1:<4} {filename:<30} {size_mb:.2f} MB {rows:<10} {created:<20}")
    
    # Summary
    total_size_mb = total_size / (1024 * 1024)
    print("-" * 80)
    print(f"Total: {len(parquet_files)} files, {total_size_mb:.2f} MB, ~{total_rows:,} rows")
    print()

def save_parquet_chunks_to_duckdb(parquet_pattern, db_path=SALES_TIMESERIES_DB):
    """
    Load all parquet files matching the pattern and save them to DuckDB
    
    Args:
        parquet_pattern: Glob pattern for the parquet files to load
        db_path: Path to the DuckDB database file
    
    Returns:
        True if successful, False otherwise
    """
    # First, list all the chunks so the user knows what will be processed
    list_parquet_chunks(parquet_pattern)
    
    print("🔍 Finding all Parquet chunks...")
    parquet_files = glob.glob(parquet_pattern)
    
    if not parquet_files:
        print("❌ No Parquet files found matching the pattern.")
        return False
    
    print(f"📂 Found {len(parquet_files)} Parquet files.")
    
    # Initialize an empty DataFrame to hold all data
    all_data = None
    total_rows = 0
    
    # Process each chunk with progress indication
    for i, file_path in enumerate(tqdm(parquet_files, desc="Loading chunks")):
        try:
            # Load the Parquet file
            chunk_df = pd.read_parquet(file_path)
            chunk_rows = len(chunk_df)
            total_rows += chunk_rows
            
            # On first chunk, initialize all_data
            if all_data is None:
                all_data = chunk_df
            else:
                # Append to existing data
                all_data = pd.concat([all_data, chunk_df], ignore_index=True)
            
            print(f"  ✅ Chunk {i+1}/{len(parquet_files)}: {chunk_rows:,} rows ({os.path.basename(file_path)})")
            
            # Periodically save to database to avoid memory issues with very large datasets
            if i > 0 and i % 5 == 0:
                print(f"💾 Intermediate save to DuckDB ({total_rows:,} rows so far)...")
                if not all_data.empty:
                    save_to_duckdb(all_data, db_path)
                    # Clear memory
                    all_data = None
                    total_rows = 0
        
        except Exception as e:
            print(f"❌ Error processing {file_path}: {str(e)}")
    
    # Final save if there's remaining data
    if all_data is not None and not all_data.empty:
        print(f"💾 Final save to DuckDB ({len(all_data):,} rows)...")
        save_to_duckdb(all_data, db_path)
    
    print("🎉 All Parquet chunks successfully loaded into DuckDB!")
    return True

def load_dataset():
    print("🔄 Loading dataset from Parquet files...")
    parquet_files = glob.glob(os.path.join(PARQUET_FILES, 'batch_*.parquet'))
    
    if not parquet_files:
        print("❌ No parquet files found. Please generate the dataset first.")
        df_all = pd.DataFrame()  # Initialize an empty DataFrame
    else:
        print(f"📂 Found {len(parquet_files)} parquet files")
        
        # Load first file to get schema
        df_all = pd.read_parquet(parquet_files[0])
        
        # Append other files
        if len(parquet_files) > 1:
            for file_path in parquet_files[1:]:
                df_chunk = pd.read_parquet(file_path)
                df_all = pd.concat([df_all, df_chunk], ignore_index=True)
        
        print(f"✅ Data loaded successfully: {len(df_all):,} rows from {len(parquet_files)} parquet files")
    return df_all                                                          
 
#def split_date_range(start_date, end_date, num_parts):
#    stime = time.mktime(time.strptime(start_date, '%Y-%m-%d %H:%M:%S'))
#    etime = time.mktime(time.strptime(end_date, '%Y-%m-%d %H:%M:%S'))
#    full_range = pd.date_range(start=stime, end=etime, freq='H')
#    ranges = []
#    chunk_size = len(full_range) // num_parts
#    for i in range(num_parts):
#        chunk_start = full_range[i * chunk_size]
#        if i == num_parts - 1:
#            chunk_end = full_range[-1]
#        else:
#            chunk_end = full_range[(i + 1) * chunk_size - 1]
#        ranges.append((chunk_start.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')))
#    return ranges

def split_hourly_range(start_datetime, end_datetime, num_parts=None):
    # Generate a range of hourly timestamps
    full_range = pd.date_range(start=start_datetime, end=end_datetime, freq='H')
    
    # Use the global CHUNK_SIZE constant
    
    # Calculate number of parts needed for 6000 records per chunk
    total_records = len(full_range)
    num_parts = max(1, (total_records + CHUNK_SIZE - 1) // CHUNK_SIZE)  # Ceiling division
    
    # Recalculate chunk size to distribute records evenly
    chunk_size = len(full_range) // num_parts
    
    print(f"⚙️ Total time points: {total_records}, using {num_parts} chunks of ~{chunk_size} records each")
    ranges = []
    for i in range(num_parts):
        chunk_start = full_range[i * chunk_size]
        if i == num_parts - 1:
            chunk_end = full_range[-1]
        else:
            chunk_end = full_range[(i + 1) * chunk_size - 1]
        ranges.append((chunk_start, chunk_end))
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
            print("    - Example: SELECT * FROM sales_data WHERE date = '2023-01-01'")
            
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
            
            # Check if we can validate that indexes exist
            try:
                # Try to run some example queries and measure performance
                print("\n⏱️ Running a sample query with EXPLAIN ANALYZE...")
                result = con.execute("""
                    EXPLAIN ANALYZE 
                    SELECT 
                        COUNT(*) 
                    FROM sales_data 
                    WHERE country = 'Singapore'
                """).fetchall()
                
                for line in result:
                    print(f"  {line[0]}")
                
            except Exception as e:
                print(f"❓ Could not analyze query performance: {str(e)}")
                
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
                    # Run the SQL query
                    result = con.execute(selected_view['sql']).fetchdf()
                    
                    if result.empty:
                        print("\n❌ No results returned.")
                    else:
                        # Set pandas display options for better output
                        pd.set_option('display.max_columns', None)
                        pd.set_option('display.width', 1000)
                        pd.set_option('display.colheader_justify', 'left')
                        pd.set_option('display.precision', 2)
                        
                        # Print results
                        print(f"\nResults: {len(result)} rows")
                        print("-" * 40)
                        print(result)
                        
                        # Ask if user wants to save results
                        save_option = input("\nSave results to CSV? (y/n): ").strip().lower()
                        if save_option == 'y':
                            # Create a valid filename from the view name
                            view_name_for_file = ''.join(c if c.isalnum() else '_' for c in selected_view['name'])
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            csv_filename = f"{view_name_for_file}_{timestamp}.csv"
                            
                            # Save to workspace directory
                            save_path = os.path.join(os.path.dirname(db_path), csv_filename)
                            result.to_csv(save_path, index=False)
                            print(f"✅ Results saved to: {save_path}")
                        
                        # Offer insights based on the view
                        print("\n💡 Key Insights:")
                        
                        if "Monthly Sales Trends" in selected_view['name']:
                            if 'revenue_change_pct' in result.columns:
                                # Revenue insights if we have non-zero revenue data
                                non_zero_revenue = result[result['revenue'] > 0].shape[0]
                                if non_zero_revenue > 0:
                                    pos_months = result[result['revenue_change_pct'] > 0].shape[0]
                                    neg_months = result[result['revenue_change_pct'] < 0].shape[0]
                                    print(f"  • Revenue: {pos_months} months with positive growth, {neg_months} months with negative growth")
                                    if not result['revenue_change_pct'].isna().all():
                                        best_month = result.loc[result['revenue_change_pct'].idxmax()]
                                        print(f"  • Best revenue growth: {best_month['month'].strftime('%B %Y')} with {best_month['revenue_change_pct']:.2f}% growth")
                                else:
                                    print("  • No revenue data available to analyze trends")
                            
                            # Customer insights
                            if 'customer_change_pct' in result.columns and not result.empty:
                                pos_cust_months = result[result['customer_change_pct'] > 0].shape[0]
                                neg_cust_months = result[result['customer_change_pct'] < 0].shape[0]
                                stable_cust_months = result[result['customer_change_pct'] == 0].shape[0]
                                print(f"  • Customers: {pos_cust_months} months with growth, {neg_cust_months} with decline, {stable_cust_months} stable")
                                
                                if not result['customer_change_pct'].isna().all():
                                    best_cust_month = result.loc[result['customer_change_pct'].idxmax()]
                                    worst_cust_month = result.loc[result['customer_change_pct'].idxmin()]
                                    print(f"  • Best customer growth: {best_cust_month['month'].strftime('%B %Y')} with {best_cust_month['customer_change_pct']:.2f}% increase")
                                    print(f"  • Largest customer decline: {worst_cust_month['month'].strftime('%B %Y')} with {worst_cust_month['customer_change_pct']:.2f}% change")
                            
                        elif "Demographics" in selected_view['name']:
                            if not result.empty and 'total_spent' in result.columns:
                                top_group = result.loc[result['total_spent'].idxmax()]
                                print(f"  • Highest spending demographic: {top_group['age_group']} ({top_group['gender']})")
                                print(f"  • This group represents {top_group['customer_count']} customers with average income ${top_group['avg_income']:,.2f}")
                        
                        elif "Top Products" in selected_view['name']:
                            if not result.empty:
                                top_product = result.iloc[0]
                                print(f"  • Top selling product: {top_product['product_name']} (ID: {top_product['product_id']})")
                                print(f"  • Generated ${top_product['total_revenue']:,.2f} in revenue from {int(top_product['total_units_sold'])} units")
                        
                        elif "Hourly Sales" in selected_view['name']:
                            if not result.empty and 'total_revenue' in result.columns:
                                peak_hour = result.loc[result['total_revenue'].idxmax()]
                                print(f"  • Peak hour is {int(peak_hour['hour_of_day']):02d}:00 with ${peak_hour['total_revenue']:,.2f} in sales")
                                print(f"  • This hour sees {int(peak_hour['transaction_count'])} transactions from {int(peak_hour['unique_customers'])} unique customers")
                        
                        elif "Geographic" in selected_view['name']:
                            if not result.empty:
                                top_country = result.iloc[0]
                                print(f"  • Top market: {top_country['country']} with ${top_country['total_revenue']:,.2f} in sales")
                                print(f"  • Represents {int(top_country['customers'])} customers making {int(top_country['transactions'])} transactions")
                        
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
                
    # Reset pandas display options to defaults
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.colheader_justify')
    pd.reset_option('display.precision')

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

def main():
    df_all = load_dataset()  # Initialize df_all to avoid reference issues later
    print("🏪 Retail TimeSeries Database Generator")
    print("=" * 40)

    while True:
        print("\n🔄 Options:")
        print("   1️⃣  Generate sample dataset in chunks of 6000 records")
        print("    2. Load dataset to DataFrame") 
        print("    3. Save to DuckDB")
        print("    4. Display dataset on screen")
        print("    5. List available Parquet chunks (metadata)")
        print("    6. Display database indexes")
        print("    7. Display field descriptions") 
        print("    8. Display database views")
        print("    9. Display saved queries/macros")
        print("    0. Exit")

        choice = input("\nSelect option (0-9): ").strip()

        if choice == '1':
            start = input("Enter start datetime (YYYY-MM-DD HH:MM:SS) [default: 1900-01-01 00:00:00]: ").strip() or '1900-01-01 00:00:00'
            end = input("Enter end datetime (YYYY-MM-DD HH:MM:SS) [default: 2025-09-02 23:00:00]: ").strip() or '2025-09-02 23:00:00'
            print("⚡ Generating new database...")

            # Split the datetime range into parts with 6000 records each
            date_ranges = split_hourly_range(start, end)
            
            # Create PARQUET directory if it doesn't exist
            os.makedirs(PARQUET_FILES, exist_ok=True)
            
            # Process chunks and collect data before saving to parquet every 4 chunks
            chunk_results = []
            collected_data = []
            chunks_per_file = 4
            
            with ProcessPoolExecutor(max_workers=4) as pool:
                futures = {}
                for i, (start_date, end_date) in enumerate(date_ranges):
                    # Generate data but don't save to parquet yet (save_to_parquet=False)
                    future = pool.submit(generate_initial_data, str(start_date), str(end_date), False)
                    futures[future] = i
                
                for fut in as_completed(futures):
                    chunk_id = futures[fut]
                    chunk_data = fut.result()  # This returns the transaction list
                    
                    if chunk_data:
                        print(f"✅ Processed chunk {chunk_id}: {len(chunk_data):,} rows")
                        collected_data.extend(chunk_data)
                        
                        # Save to parquet every 4 chunks or when we reach the end
                        if len(collected_data) >= chunks_per_file * CHUNK_SIZE or chunk_id == len(date_ranges) - 1:
                            # Convert collected data to DataFrame and save
                            df_batch = pd.DataFrame(collected_data)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            parquet_filename = os.path.join(PARQUET_FILES, f'batch_{timestamp}.parquet')
                            
                            print(f"💾 Saving batch to {parquet_filename}: {len(df_batch):,} rows")
                            df_batch.to_parquet(parquet_filename, index=False)
                            
                            # Store metadata
                            chunk_results.append({
                                'chunk_id': len(chunk_results),
                                'file_path': parquet_filename,
                                'rows': len(df_batch),
                                'created_at': timestamp
                            })
                            
                            # Clear collected data for next batch
                            collected_data = []
            
            # Save metadata
            metadata_path = os.path.join(PARQUET_FILES, 'chunks_metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(chunk_results, f, indent=2)
                
            print(f"✅ Database generation complete! {len(chunk_results)} parquet batch files created.")
            
            # Ask if the user wants to save all chunks to DuckDB
            save_to_db = input("\nDo you want to save all chunks to DuckDB? (y/n): ").strip().lower()
            if save_to_db == 'y':
                print("🔄 Loading all parquet chunks into DuckDB...")
                save_parquet_chunks_to_duckdb(os.path.join(PARQUET_FILES, 'batch_*.parquet'))
        
        elif choice == '2':
            print("🔄 Loading dataset from Parquet files...")
            # Use the predefined PARQUET_FILES variable instead of creating a new PARQUET_DIR
            parquet_files = glob.glob(os.path.join(PARQUET_FILES, 'batch_*.parquet'))
            
            if not parquet_files:
                print("❌ No parquet files found. Please generate the dataset first.")
                df_all = pd.DataFrame()  # Initialize an empty DataFrame
            else:
                print(f"📂 Found {len(parquet_files)} parquet files")
                
                # Load first file to get schema
                df_all = pd.read_parquet(parquet_files[0])
                
                # Append other files
                if len(parquet_files) > 1:
                    for file_path in parquet_files[1:]:
                        df_chunk = pd.read_parquet(file_path)
                        df_all = pd.concat([df_all, df_chunk], ignore_index=True)
                
                print(f"✅ Data loaded successfully: {len(df_all):,} rows from {len(parquet_files)} parquet files")
        
        
        elif choice == '3':
            print('💾 Saving Parquet chunks to DuckDB database...')
            save_parquet_chunks_to_duckdb(os.path.join(PARQUET_FILES, 'batch_*.parquet'))

        elif choice == '4':
            print("\n📊 Displaying Sample Dataset")
            print("=" * 40)
            df_all = load_dataset()
            
            if df_all.empty:
                print("❌ No data available. Please load or generate a dataset first.")
                continue
                
            # Get total rows and show sample size
            total_rows = len(df_all)
            sample_rows = min(5, total_rows)  # Show at most 5 rows
            print(f"Total records: {total_rows:,} | Showing first {sample_rows} records\n")
            
            # Display the actual data
            print(df_all.head(sample_rows).to_string(index=False))
            print(f"\nDataset shape: {df_all.shape}")
            print(f"Columns: {list(df_all.columns)}")
            
        elif choice == '5':
            # List all available Parquet chunks
            list_parquet_chunks()
            
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
                for col in columns:
                    if col in df_all.columns:
                        i = df_all.columns.get_loc(col) + 1  # 1-based index
                        dtype = str(df_all[col].dtype)
                        print(f"  {i}. {col:<25} [{dtype}]")
            
            # Format values for display
            def format_value(val):
                if pd.isna(val):
                    return ""
                elif isinstance(val, (int, float)) and not isinstance(val, bool):
                    if isinstance(val, float):
                        return f"{val:,.2f}"
                    return f"{val:,}"
                return str(val)
            
            # Print data in a multi-column layout
            print("\nData sample (Records with grouped fields):")
            print("=" * 40)
            
            for row_idx in range(sample_rows):
                if row_idx < len(df_all):
                    row = df_all.iloc[row_idx]
                    print(f"\n📄 Record #{row_idx + 1}:")
                    print("-" * 40)
                    
                    for g_idx, (group_name, columns) in enumerate(zip(group_names, column_groups)):
                        print(f"\n  {group_name}:")
                        for col in columns:
                            if col in df_all.columns:
                                val = format_value(row[col])
                                print(f"    • {col:<25}: {val}")
                        
                    print("-" * 40)
                
                # Reset pandas display options to defaults
                pd.reset_option('display.max_columns')
                pd.reset_option('display.width')
                pd.reset_option('display.precision')
                pd.reset_option('display.max_colwidth')
                pd.reset_option('display.colheader_justify')
        elif choice == '6':
            display_database_indexes()
        elif choice == '7':
            display_field_descriptions()
        elif choice == '8':
            display_db_views()
        elif choice == '9':
            display_db_saved_queries()
        elif choice == '0':
            print("👋 Goodbye!")
        else:
            print("❌ Invalid choice. Please select a valid option.")


if __name__ == "__main__":
    main()
    sys.exit(0)
      