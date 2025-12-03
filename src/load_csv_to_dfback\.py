import duckdb as ddb
import os

OUTPUT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) 
EXPORTED_JSON = os.path.join(OUTPUT_ROOT, 'src', 'params_to_save.json')
EXPORTED_CSV = os.path.join(OUTPUT_ROOT, 'src', 'exported_dataset.csv')
EXPORTED_DB = os.path.join(OUTPUT_ROOT, 'src', 'exported_dataset.db')
TABLE_NAME = 'exported_dataset'
TABLE_NAME_TEMP = 'parameters'   
required_columns = [
    'date', 'transaction_id', 'transaction_desc', 'customer_id', 
    'age', 'gender', 'receipt_number', 'product_id', 'product_name', 
    'units_sold', 'unit_price_sgd', 'total_amount_per_product_sgd', 
    'receipt_total_sgd', 'country_id', 'country', 'city', 'income'
]

def load_csv_to_df(tablename:str,importedfile:str,exportedfile:str):
    with ddb.connect(exportedfile) as conn:
        # Load CSV into DuckDB table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {tablename} AS 
            SELECT * FROM read_csv_auto('{importedfile}')
        """)
  

    # return the duckdb dataframe
    return ddb.connect(exportedfile).table(tablename)
        
def sample_data():
    file_path = EXPORTED_JSON
    file_path2 = EXPORTED_CSV
    print(f"📊 Loading data from: {file_path}")
    print(f"🗄️  Database: {EXPORTED_DB}")
    
    with ddb.connect(EXPORTED_DB) as conn:
        # Create table from CSV if it doesn't exist
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} AS SELECT * FROM '{file_path2}' ")
        conn.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME_TEMP} AS SELECT * FROM '{file_path}' ")
        conn.execute(f"PRAGMA table_info('{TABLE_NAME}')")
        conn.commit()
                        
        # Show available tables
        print("\n📋 Available tables:")
        tables = conn.execute("SHOW TABLES").fetchall() 
        for table_row in tables:
            table_name = table_row[0]  # Extract table name from tuple
            print(f"***** TABLE *****************  - {table_name}")
        
            # Show table schema
            print(f"\n🏗️  Table Schema for '{table_name}':")
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()
            for col in schema:
                print(f"  - {col[0]} ({col[1]}) {'NULLABLE' if col[2] == 'YES' else 'NOT NULL'}")

            # Show table statistics
            print(f"\n📈 Table Statistics for '{table_name}':")
            stats = conn.execute(f"SELECT COUNT(*) as total_rows FROM {table_name}").fetchone()
            print(f"  Total rows: {stats[0]:,}")

            # Show first 10 rows
            print(f"\n🔍 First 10 rows from '{table_name}':")
            results = conn.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchall()

            if results:
                # Print column headers
                columns = [desc[0] for desc in conn.description]
                print("  " + " | ".join(f"{col[:15]:<15}" for col in columns))
                print("  " + "-" * (len(columns) * 18))

                # Print data rows
                for row in results:
                    formatted_row = []
                    for val in row:
                        if val is None:
                            formatted_row.append("NULL")
                        else:
                            formatted_row.append(str(val)[:15])
                    print("  " + " | ".join(f"{val:<15}" for val in formatted_row))
            else:
                print("  No data found in table")
        
           
           
    
           
        
        
        
           
           
           
            print("-" * 80)  # Separator between tables
            
    print("\n✅ DuckDB table analysis complete!")

def main():
    load_csv_to_df(tablename=TABLE_NAME,importedfile=EXPORTED_CSV,exportedfile=EXPORTED_DB)
    
if __name__ == "__main__":
    main()