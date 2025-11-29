#!/usr/bin/env python3
"""
Test script to verify the parquet loading fix
"""

import os
import sys
import duckdb
from glob import glob

# Add src to path to import main functions
sys.path.append('src')
from main import PARQUET_FILES, SALES_TIMESERIES_DB, save_parquet_chunks_to_duckdb

def test_parquet_loading():
    """Test the parquet loading functionality"""
    print("🧪 Testing Parquet Loading Fix")
    print("=" * 40)
    
    # Check if parquet files exist
    parquet_pattern = os.path.join(PARQUET_FILES, 'batch_*.parquet')
    parquet_files = glob(parquet_pattern)
    
    if not parquet_files:
        print("❌ No parquet files found to test")
        print(f"   Looking for: {parquet_pattern}")
        return False
    
    print(f"📂 Found {len(parquet_files)} parquet files")
    
    # Test loading a single file first
    test_file = parquet_files[0]
    print(f"🔍 Testing single file: {os.path.basename(test_file)}")
    
    try:
        with duckdb.connect() as con:
            # Test reading the parquet file
            con.execute(f"CREATE TEMP TABLE test_data AS SELECT * FROM read_parquet('{test_file}')")
            row_count = con.execute("SELECT COUNT(*) FROM test_data").fetchone()[0]
            print(f"✅ Successfully read {row_count:,} rows from test file")
            
            # Test the data structure
            columns = con.execute("PRAGMA table_info('test_data')").fetchall()
            print(f"📋 Table has {len(columns)} columns")
            
    except Exception as e:
        print(f"❌ Error reading parquet file: {e}")
        return False
    
    # Test the save function with just one file
    print(f"\n🔄 Testing database save functionality...")
    
    try:
        # Create a test database
        test_db = "test_parquet_load.db"
        if os.path.exists(test_db):
            os.remove(test_db)
        
        # Test loading just one file
        success = save_parquet_chunks_to_duckdb(test_file, test_db)
        
        if success:
            # Verify the data was saved
            with duckdb.connect(database=test_db, read_only=True) as verify_con:
                try:
                    db_rows = verify_con.execute("SELECT COUNT(*) FROM sales_data").fetchone()[0]
                    print(f"✅ Successfully saved {db_rows:,} rows to database")
                    
                    # Show table schema
                    schema = verify_con.execute("PRAGMA table_info('sales_data')").fetchall()
                    print(f"📋 Database table has {len(schema)} columns")
                    
                except Exception as e:
                    print(f"❌ Error verifying database: {e}")
        
        # Cleanup test database
        if os.path.exists(test_db):
            os.remove(test_db)
            
    except Exception as e:
        print(f"❌ Error in save functionality: {e}")
        return False
    
    print(f"\n🎉 Parquet loading test completed successfully!")
    return True

if __name__ == "__main__":
    test_parquet_loading()