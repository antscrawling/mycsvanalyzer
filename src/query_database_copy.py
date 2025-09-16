import duckdb
import pandas as pd
from pprint import pprint


# Connect to the database file


class SalesData:
    def __init__(self):
        df = pd.DataFrame()
    
    
       
       


def querry_sales_data():
    with duckdb.connect("src/sales_timeseries.db",read_only=True) as con:
        # Query the data        
        print("TABLE = sales_data")
        results = con.execute("SELECT * FROM sales_data").fetchall()
    # print(con.execute("SELECT COUNT(*) FROM sales_data").fetchall())
        #convert to df
        df = pd.DataFrame(results, columns=[desc[0] for desc in con.description])
        

        #df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d %H:%M:%S')
    # print(df['date'].dt.strftime('%Y-%m-%d %H:%M:%S'), df['customer_id'], df['total_amount_per_product_sgd'])
        
    # print("\n=== Sample Data ===")
    # df = con.execute("SELECT age,customer_id,gender  FROM sales_data").df()
        print(df)
    # #print customer id , age, and sales for each age group
    # 
    # #print(df['customer_number'][(df['age'] > 25) & (df['age'] <= 50)])
    # print(df['age'][(df['age'] > 25) & (df['age'] <= 50)])
    # print(df['total_amount_per_product_sgd'][(df['age'] > 25) & (df['age'] <= 50)])

def querry_parameters():
    with duckdb.connect("src/parameters.db",read_only=True) as con:
        con.execute("PRAGMA show_tables").fetchall()
        print("TABLE = parameters")
        results = con.execute("SELECT * FROM parameters").fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in con.description])
        pprint(df)
    
    
    
def main():
    #querry_sales_data()
    querry_parameters()
    
if __name__ == "__main__":
    main()
    
    
    
    
    