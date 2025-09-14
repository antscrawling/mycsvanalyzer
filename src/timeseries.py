import json
import pandas as pd
import numpy as np
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
        df = transform_data(csvfile=csv_file)
    create_duckdb_table(df)
    print(df.head(5))
    print(df.info())
    print(df.describe())   
    df[df['age'] > 30] 

def transform_data(csvfile : str)->pd.DataFrame:
    
    # 1. Load CSV file into DataFrame
    df = pd.read_csv(csvfile,header=0)
    
    ## 2. Convert 'date' column to numeric format
    #df['date'] = df['date'].apply(lambda x: int(str(x).replace("-", "").replace(":", "").replace(" ", ""))).astype(np.int64)
    #
    ## 3. Convert non-numeric columns to integers using factorization
    #for col in df.columns:
    #    if df[col].dtype == 'object':  # Check if the column is non-numeric
    #        df[col], uniques = pd.factorize(df[col])  # Factorize the column
    #        print(f"Column '{col}' factorized. Unique values: {list(uniques)}")
    #    else :
    #        continue
    ## 4. Print DataFrame info after transformation
    #print(f"Transformed DataFrame:\n{df.info()}")
    return df


   
    
    
#def serialize_column_mappings(df):
#    mappings = {}
#    for col in df.select_dtypes(include=['object']).columns:
#        unique_values = df[col].dropna().unique()
#        col_mapping = {idx: value for idx, value in enumerate(unique_values)}
#        mappings[col] = col_mapping
#        df[col] = df[col].map({value: idx for idx, value in col_mapping.items()})
#    return mappings  
     

def create_duckdb_table(df, table_name="all_data"):    
    # 4. Use DuckDB to store the transformed data
    with duckdb.connect(database='transformed_data.db', read_only=False) as con:
        table_name = table_name
    
        # Create the table if it doesn't exist
        con.register('df_view', df)
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df_view")                             
        records = len(df)
        # Insert data into the table
        con.execute(f"INSERT INTO {table_name} SELECT * FROM df_view")
        con.unregister('df_view')  # Unregister the view after use


    print(f"Processed {records} records inserted into DuckDB and mappings saved to ")

if __name__ == "__main__":
    main()