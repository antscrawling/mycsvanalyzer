import json
import os
import duckdb
from pprint import pprint
import pandas as pd



def main():
    save_the_parameters_to_db()

def convert_parameters_to_class():
    import json
    import pandas as pd
    import numpy as np
    import duckdb
    from datetime import datetime, timedelta
    from tqdm import tqdm
    import os
    import sys
    from pprint import pprint
    import random
    from pydantic import BaseModel, Field, ValidationError
    import ast
    import inspect
    
    with duckdb.connect('src/parameter.db',read_only=True) as con:
        con.execute('PRAGMA show_tables').fetchall()
        print('TABLE = parameters')
        results = con.execute('SELECT * FROM parameters').fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in con.description])
        pprint(df)
        mydata = {}
        for index, row in df.iterrows():
            mydata[row['key']] = json.loads(row['value'])
        pprint(mydata)
        
    class Parameters(BaseModel):
        '''This class will hold all the parameters for the data generation.'''
        mydataframe : pd.DataFrame = Field(default_factory=pd.DataFrame)
        np_random_seed: int = Field(default=42)       
        genders: list[str] = Field(default_factory=list)
        products: list[dict[str, float]] = Field(default_factory=list)
        age_groups: list[str] = Field(default_factory=list)
        age_weights: list[float] = Field(default_factory=list)
        age_range_start: int = Field(default=0)
        customer_ages: dict[int, int] = Field(default_factory=dict)
        city: str = Field(default='')
        selected_city: dict[str, str] = Field(default_factory=dict)
        incomes: list[int] = Field(default_factory=list)
        customer_income: int = Field(default=0)
        items_per_receipt: int = Field(default=0)
        selected_products: list[int] = Field(default_factory=list)
        transaction_types: list[str] = Field(default_factory=list)
        cities: list[dict[str, str]] = Field(default_factory=list)
        unit_price: float = Field(default=0.0)
        total_amount_per_product: float = Field(default=0.0)
        receipt_total: float = Field(default=0.0)
        hour: int = Field(default=0)
        transaction_datetime: datetime = Field(default_factory=datetime.now)
        transaction_id: int = Field(default=0)
        OUTPUT_ROOT: str = Field(default='')
        SRC_FILES: str = Field(default='')
        BIG_CSV_FILES: str = Field(default='')
        SPLIT_CSV_FOLDER: str = Field(default='')
        SPLIT_CSV_FILES: str = Field(default='')
        PARQUET_FILES: str = Field(default='')
        ZLIB_FILES: str = Field(default='')
        CSV_FILE_EXTENSION: str = Field(default='')
        PARQUET_FILE_EXTENSION: str = Field(default='')
        ZLIB_FILE_EXTENSION: str = Field(default='')
        SPLIT_CSV_PATH: str = Field(default='')
        PARQUET_PATH: str = Field(default='')
        ZLIB_PATH: str = Field(default='')
        SOURCE_DIR: str = Field(default='')
        CITIES_JSON: str = Field(default='')
        PARAM_DB: str = Field(default='')
        ANALYZED_DB: str = Field(default='')
        CHATGPT_RESULTS: str = Field(default='')
        ANALYZED_CSV: str = Field(default='')
        SALES_TIMESERIES_CSV: str = Field(default='')
        SALES_TIMESERIES_DB: str = Field(default='')

        
  
        def load_parameters_from_duckdb(self):
            '''This program will evaluate the given DataFrame and return the results.'''
   
            with duckdb.connect('src/parameter.db',read_only=True) as con:
                con.execute('PRAGMA show_tables').fetchall()
                print('TABLE = parameters')
                results = con.execute('SELECT * FROM parameters').fetchall()
                self.mydataframe= pd.DataFrame(results, columns=[desc[0] for desc in con.description])
                pprint(self.mydataframe)
                mydata = {}
                for index, row in df.iterrows():
                    mydata[row['key']] = json.loads(row['value'])
                pprint(mydata)
            
            
            
   
   
   
   
   
    
def save_the_parameters_to_db():
    print('💾 Save Parameter Utility')
    print('=' * 30)

    mydata = {
        'genders': ['M', 'F'],
        'products': [
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
        ],
        'age_groups': ['18-25', '26-35', '36-45', '46-55', '56-65', '66+'],
        'age_weights': [0.05, 0.20, 0.25, 0.25, 0.15, 0.08, 0.02],
        'age_range_start': 'np.random.choice(age_ranges, p=age_weights)',
        'customer_ages[customer_id]': 'np.random.randint(age_range_start, min(age_range_start + 10, 80))',
        'city': "np.random.choice([city['name'] for city in cities])",
        'selected_city': "next(item for item in cities if item['name'] == city)",
        'incomes': [30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000, 100_000, 120_000, 150_000, 200_000],
        'customer_income': 'np.random.choice(range(min(incomes),max(incomes)),1)',
        'customer_ages[customer_id]': 'customer_income',
        'items_per_receipt': 'np.random.choice([1, 2, 3, 4], p=[0.4, 0.3, 0.2, 0.1])',
        'selected_products': 'np.random.choice(len(products), size=items_per_receipt, replace=False)',
        'transaction_types': ['Product Sale','Product Refund','Product Exchange'],
        'cities': [
            {'name': 'New York', 'country': 'United States', 'country_id': 'C001'},
            {'name': 'Tokyo', 'country': 'Japan', 'country_id': 'C002'},
            {'name': 'Paris', 'country': 'France', 'country_id': 'C003'},
            {'name': 'London', 'country': 'United Kingdom', 'country_id': 'C004'},
            {'name': 'Sydney', 'country': 'Australia', 'country_id': 'C005'},
            {'name': 'Berlin', 'country': 'Germany', 'country_id': 'C006'},
            {'name': 'Toronto', 'country': 'Canada', 'country_id': 'C007'},
            {'name': 'Beijing', 'country': 'China', 'country_id': 'C008'},
            {'name': 'Moscow', 'country': 'Russia', 'country_id': 'C009'},
            {'name': 'Rio de Janeiro', 'country': 'Brazil', 'country_id': 'C010'},
            {'name': 'Cape Town', 'country': 'South Africa', 'country_id': 'C011'},
            {'name': 'Mumbai', 'country': 'India', 'country_id': 'C012'},
            {'name': 'Mexico City', 'country': 'Mexico', 'country_id': 'C013'},
            {'name': 'Buenos Aires', 'country': 'Argentina', 'country_id': 'C014'},
            {'name': 'Cairo', 'country': 'Egypt', 'country_id': 'C015'},
            {'name': 'Bangkok', 'country': 'Thailand', 'country_id': 'C016'},
            {'name': 'Istanbul', 'country': 'Turkey', 'country_id': 'C017'},
            {'name': 'Seoul', 'country': 'South Korea', 'country_id': 'C018'},
            {'name': 'Singapore', 'country': 'Singapore', 'country_id': 'C019'},
            {'name': 'Dubai', 'country': 'United Arab Emirates', 'country_id': 'C020'},
            {'name': 'Jakarta', 'country': 'Indonesia', 'country_id': 'C021'},
            {'name': 'Lagos', 'country': 'Nigeria', 'country_id': 'C022'},
            {'name': 'Nairobi', 'country': 'Kenya', 'country_id': 'C023'},
            {'name': 'Kuala Lumpur', 'country': 'Malaysia', 'country_id': 'C024'},
            {'name': 'Madrid', 'country': 'Spain', 'country_id': 'C025'},
            {'name': 'Rome', 'country': 'Italy', 'country_id': 'C026'},
            {'name': 'Athens', 'country': 'Greece', 'country_id': 'C027'},
            {'name': 'Lisbon', 'country': 'Portugal', 'country_id': 'C028'},
            {'name': 'Hanoi', 'country': 'Vietnam', 'country_id': 'C029'},
            {'name': 'Manila', 'country': 'Philippines', 'country_id': 'C030'},
            {'name': 'Santiago', 'country': 'Chile', 'country_id': 'C031'},
            {'name': 'Bogotá', 'country': 'Colombia', 'country_id': 'C032'},
            {'name': 'Lima', 'country': 'Peru', 'country_id': 'C033'},
            {'name': 'Caracas', 'country': 'Venezuela', 'country_id': 'C034'},
            {'name': 'Warsaw', 'country': 'Poland', 'country_id': 'C035'},
            {'name': 'Prague', 'country': 'Czech Republic', 'country_id': 'C036'},
            {'name': 'Vienna', 'country': 'Austria', 'country_id': 'C037'},
            {'name': 'Budapest', 'country': 'Hungary', 'country_id': 'C038'},
            {'name': 'Stockholm', 'country': 'Sweden', 'country_id': 'C039'},
            {'name': 'Oslo', 'country': 'Norway', 'country_id': 'C040'},
            {'name': 'Copenhagen', 'country': 'Denmark', 'country_id': 'C041'},
            {'name': 'Helsinki', 'country': 'Finland', 'country_id': 'C042'},
            {'name': 'Reykjavik', 'country': 'Iceland', 'country_id': 'C043'},
            {'name': 'Zurich', 'country': 'Switzerland', 'country_id': 'C044'},
            {'name': 'Brussels', 'country': 'Belgium', 'country_id': 'C045'},
            {'name': 'Amsterdam', 'country': 'Netherlands', 'country_id': 'C046'},
            {'name': 'Dublin', 'country': 'Ireland', 'country_id': 'C047'},
            {'name': 'Edinburgh', 'country': 'Scotland', 'country_id': 'C048'},
            {'name': 'Glasgow', 'country': 'Scotland', 'country_id': 'C049'}
        ],
        'unit_price': "product['unit_price'] * np.random.uniform(0.9, 1.1)",
        'total_amount_per_product': 'units_sold * unit_price',
        'receipt_total': '+= total_amount_per_product',
        'hour': 'np.random.randint(6, 22)',  
        'transaction_datetime': 'date + timedelta(hours=hour, minutes=np.random.randint(0, 60))',
        'transaction_id': 'np.random.randint(1000000, 9999999)',
        'OUTPUT_ROOT' : "os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))",
        'SRC_FILES' : "os.path.join(OUTPUT_ROOT, 'src')",  
        'BIG_CSV_FILES' : "os.path.join(OUTPUT_ROOT, 'src')",
        'SPLIT_CSV_FOLDER' : 'SPLITCSV'  ,
        'SPLIT_CSV_FILES' : "os.path.join(SRC_FILES, 'SPLITCSV')",
        'PARQUET_FILES' : "os.path.join(OUTPUT_ROOT, 'src', 'PARQUET')",
        'ZLIB_FILES' : "os.path.join(OUTPUT_ROOT, 'src', 'ZLIB')",
        'CSV_FILE_EXTENSION'      : 'csv' ,
        'PARQUET_FILE_EXTENSION'  : 'parquet',
        'ZLIB_FILE_EXTENSION'     : 'zlib',
        'SPLIT_CSV_PATH'  : 'SPLIT_CSV_FILES' , 
        'PARQUET_PATH'    : "os.path.join(SRC_FILES, PARQUET_FILES)",
        'ZLIB_PATH'       : "os.path.join(SRC_FILES, ZLIB_FILES)",
        'SOURCE_DIR'      : "os.path.join(OUTPUT_ROOT,'src')",
        'CITIES_JSON'     : "os.path.join(SOURCE_DIR,'cities.json')",
        'PARAM_DB'        : "os.path.join(SOURCE_DIR, 'parameter.db')",
        'ANALYZED_DB'     : "os.path.join(SOURCE_DIR, 'analyzed_database.db')",
        'CHATGPT_RESULTS' : "os.path.join(SOURCE_DIR, 'chatgpt_results.json')",
        'ANALYZED_CSV'    : "os.path.join(SOURCE_DIR, 'analyzed_data.csv')",
        'SALES_TIMESERIES_CSV' : "os.path.join(SOURCE_DIR, 'sales_timeseries.csv')",
        'SALES_TIMESERIES_DB' : "os.path.join(SOURCE_DIR, 'sales_timeseries.db')"
        }

    df = pd.DataFrame(list(mydata.items()), columns=['key', 'value'])
    
    with duckdb.connect('src/parameters.db') as con:
        con.execute('CREATE TABLE IF NOT EXISTS parameters (key VARCHAR, value VARCHAR)')
        con.execute('DELETE FROM parameters')  # Clear existing data
        for key, value in mydata.items():
            con.execute('INSERT INTO parameters (key, value) VALUES (?, ?)', (key, json.dumps(value)))
        print('✅ Parameters saved to parameters.db')
        results = con.execute('SELECT * FROM parameters').fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in con.description])
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pprint(df)
if __name__ == '__main__':
    main()
    #convert_parameters_to_class()
