
import duckdb as ddb

def load_csv_to_df(file_path: str):
    # Load data using DuckDB and return connection with table
    con = ddb.connect(':memory:')
    con.execute(f"CREATE TABLE loaded_data AS SELECT * FROM read_csv_auto('{file_path}')")
    return con

def main():
    file_path = "src/sales_timeseries.csv"
    df = load_csv_to_df(file_path)
    print(df)
    return df
    
if __name__ == "__main__":
    main()
