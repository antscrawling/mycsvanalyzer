import json
import duckdb
from datetime import datetime, timedelta
from tqdm import tqdm
from pprint import pprint




def main():
    #import the csv name of file
    # chunk_1.csv to chunk_384.csv
    csv_files = [f"src/chunk_{i}.csv" for i in range(1,384)]
    for csv_file in csv_files:
        print(f'Processing file: {csv_file}')
        con = transform_data(csvfile=csv_file)
        
        if con is not None:
            create_duckdb_table(con)
            
            # Display sample data using DuckDB
            sample_data = con.execute("SELECT * FROM csv_data LIMIT 5").df()
            print(sample_data)
            
            # Display info using DuckDB
            table_info = con.execute("PRAGMA table_info('csv_data')").df()
            print("Table Info:")
            print(table_info)
            
            # Display statistics
            numeric_stats = con.execute("SELECT * FROM csv_data WHERE age > 30 LIMIT 5").df()
            print("Records where age > 30:")
            print(numeric_stats)
            
            con.close()
        else:
            print(f"Skipped {csv_file} due to loading error") 

def transform_data(csvfile: str):
    
    # 1. Load CSV file using DuckDB and create a persistent connection
    con = duckdb.connect(':memory:')
    try:
        con.execute(f"CREATE TABLE csv_data AS SELECT * FROM read_csv_auto('{csvfile}')")
        return con
    except Exception as e:
        print(f"Error loading {csvfile}: {e}")
        con.close()
        return None


   
    
    
#def serialize_column_mappings(df):
#    mappings = {}
#    for col in df.select_dtypes(include=['object']).columns:
#        unique_values = df[col].dropna().unique()
#        col_mapping = {idx: value for idx, value in enumerate(unique_values)}
#        mappings[col] = col_mapping
#        df[col] = df[col].map({value: idx for idx, value in col_mapping.items()})
#    return mappings  
     

def create_duckdb_table(source_con, table_name="all_data"):    
    if source_con is None:
        print("No connection provided, skipping table creation")
        return
        
    # 4. Use DuckDB to store the transformed data
    with duckdb.connect(database='transformed_data.db', read_only=False) as target_con:
        # Create the table if it doesn't exist
        target_con.execute(f"DROP TABLE IF EXISTS {table_name}")
        target_con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM csv_data")
        
        # Get record count
        records = source_con.execute("SELECT COUNT(*) FROM csv_data").fetchone()[0]
        
    print(f"Processed {records} records inserted into DuckDB table {table_name}")

if __name__ == "__main__":
    main()