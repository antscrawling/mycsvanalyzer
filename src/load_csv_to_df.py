
import pandas as pd
import duckdb as ddb

def load_csv_to_df(file_path: str) -> pd.DataFrame:
    # Load CSV file into a DuckDB table
    con = ddb.connect()
    #con.execute(f"CREATE TABLE temp_table AS SELECT * FROM read_csv_auto('{file_path}')")
    ## Query the table into a DataFrame
    df = con.execute("SELECT * FROM sales_data").fetchdf()
    con.close()
    return df

def main():
    file_path = "src/sales_timeseries.csv"
    df = load_csv_to_df(file_path)
    print(df)
    return df
    
if __name__ == "__main__":
    main()
