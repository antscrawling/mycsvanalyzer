import json
import os
import pickle
from pprint import pprint



def main():
    save_the_parameters_to_db()

    
def save_the_parameters_to_db():
    print('💾 Save Parameter Utility')
    print('=' * 30)

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
    #save the constants into a json file
    
    
    
    
    #print({k: f"{v}" for k, v in locals().items()})
   # df = pd.DataFrame(list(mydata.items()), columns=['key', 'value'])
   # 
   # with duckdb.connect('src/parameters.db') as con:
   #     con.execute('CREATE TABLE IF NOT EXISTS parameters (key VARCHAR, value VARCHAR)')
   #     con.execute('DELETE FROM parameters')  # Clear existing data
   #     for key, value in mydata.items():
   #         con.execute('INSERT INTO parameters (key, value) VALUES (?, ?)', (key, json.dumps(value)))
   #     print('✅ Parameters saved to parameters.db')
   #     results = con.execute('SELECT * FROM parameters').fetchall()
   #     df = pd.DataFrame(results, columns=[desc[0] for desc in con.description])
   #     pd.set_option('display.max_columns', None)
   #     pd.set_option('display.max_rows', None)
   #     pprint(df)
    
    # Define which variables to save (only the path/config variables)
    params_to_save = {
        'VALUE_A' : 42,
        'VALUE_B' : 3,
        'VALUE_C' : 't"The value of C = {VALUE_A * VALUE_B}"',
        'OUTPUT_ROOT': OUTPUT_ROOT,
        'SRC_FILES': SRC_FILES,
        'BIG_CSV_FILES': BIG_CSV_FILES,
        'SPLIT_CSV_FOLDER': SPLIT_CSV_FOLDER,
        'SPLIT_CSV_FILES': SPLIT_CSV_FILES,
        'PARQUET_FILES': PARQUET_FILES,
        'ZLIB_FILES': ZLIB_FILES,
        'CSV_FILE_EXTENSION': CSV_FILE_EXTENSION,
        'PARQUET_FILE_EXTENSION': PARQUET_FILE_EXTENSION,
        'ZLIB_FILE_EXTENSION': ZLIB_FILE_EXTENSION,
        'CHUNK_SIZE': CHUNK_SIZE,
        'SPLIT_CSV_PATH': SPLIT_CSV_PATH,
        'PARQUET_PATH': PARQUET_PATH,
        'ZLIB_PATH': ZLIB_PATH,
        'SOURCE_DIR': SOURCE_DIR,
        'CITIES_JSON': CITIES_JSON,
        'PARAM_DB': PARAM_DB,
        'ANALYZED_DB': ANALYZED_DB,
        'CHATGPT_RESULTS': CHATGPT_RESULTS,
        'ANALYZED_CSV': ANALYZED_CSV,
        'SALES_TIMESERIES_CSV': SALES_TIMESERIES_CSV,
        'SALES_TIMESERIES_DB': SALES_TIMESERIES_DB,
        'SALES_TIMESERIES_PICKLE': SALES_TIMESERIES_PICKLE
    }

    with open(os.path.join(SOURCE_DIR, 'params_to_save.json'), 'w') as file:
        json.dump(params_to_save, file, indent=4)
if __name__ == '__main__':
    main()
    #convert_parameters_to_class()
