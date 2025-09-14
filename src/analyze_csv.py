import duckdb
import pandas as pd
import numpy as np
import os
import sys
import json
import requests
from pprint import pprint

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
PARAM_DB = 'src/parameter.db'
ANALYZED_DB = 'src/analyzed_database.db'
ANALYZED_TABLE = 'analyzed_data'
PARAM_TABLE = 'analysis_parameters'
CHATGPT_RESULTS = 'src/chatgpt_results.json'
PACKED_CSV = 'src/packed_data.zlib'
UNPACKED_CSV = 'src/unpacked_data.csv'
CITIES_LOCATION = 'src/cities.json'
COUNTRIES_LOCATION = 'src/countries.json'

# Function 1: Load CSV File for Analysis
def load_csv_file():
    print("Available CSV files in the 'src' directory:")
    csv_files = [f for f in os.listdir('src') if f.endswith('.csv')]
    for i, file in enumerate(csv_files):
        print(f"{i+1}. {file}")
    print(f"{len(csv_files)+1}. Exit")  # Add exit option

    choice_file = input(f"Select a file (1-{len(csv_files)+1}) or enter filename: ").strip()
    if choice_file.isdigit():
        index = int(choice_file) - 1
        if index == len(csv_files):
            print("Exiting file selection.")
            return None
        if 0 <= index < len(csv_files):
            csv_file = os.path.join('src', csv_files[index])
        else:
            print("Invalid choice.")
            return None
    else:
        csv_file = os.path.join('src', choice_file)
    if not os.path.isfile(csv_file):
        print("File not found.")
        return None
    df = pd.read_csv(csv_file)
    print("CSV Data Loaded:")
    print(df.head())
    print(f"DataFrame shape: {df.shape}")
    return df

# Function 2: Manual Analysis
def manual_analysis(dataframe: pd.DataFrame, parameters: dict):
    for column, dtype in parameters.items():
        if column in dataframe.columns:
            try:
                if isinstance(dtype, dict):
                    dtype = dtype.get('value', '')
                if not isinstance(dtype, str):
                    print(f"Skipping column {column} due to invalid dtype: {dtype}")
                    continue
                if 'date' in column.lower() or 'time' in column.lower():
                    dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
                elif 'integer' in dtype.lower():
                    dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce', downcast='integer')
                elif 'float' in dtype.lower():
                    dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce', downcast='float')
                elif 'character' in dtype.lower() or 'string' in dtype.lower():
                    dataframe[column] = dataframe[column].astype(str)
                else:
                    print(f"Unknown data type for column {column}: {dtype}")
            except Exception as e:
                print(f"Error converting column {column} to {dtype}: {e}")
    pprint(dataframe)

# Function 3: ChatGPT Analysis
def analyze_csv(dataframe: pd.DataFrame, params: dict):
    df = dataframe
    if df is None or df.empty:
        print("No data to analyze.")
        return
    myparams = [f"{key}: {value}" for key, value in params.items()]
    mycolumns = df.columns.tolist()
    print("\nCSV Header:")
    for column in mycolumns:
        print(f" - {column}")
    try:
        json_response = get_similar_columns_chatgpt(paramlist=myparams, columnlist=mycolumns)
        myresults = json.loads(json_response)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        print(f"Raw response: {json_response}")
        myresults = {"error": "Failed to parse JSON response"}
    with open(CHATGPT_RESULTS, 'w') as f:
        json.dump(myresults, f, indent=4)
    with open(CHATGPT_RESULTS, 'r') as f:
        json_data = json.load(f)
        for column in df.columns:
            if column in json_data:
                if 'date' in column.lower() or 'time' in column.lower():
                    df[column] = pd.to_datetime(df[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
                df[column] = df[column].astype(json_data[column])
    print("\nAnalyzed DataFrame Info:")
    print(df.info())

# Function 4: Display Analysis
def display_proposed_analysis(dataframe, json_file: str):
    if not os.path.exists(json_file):
        print(f"File {json_file} not found.")
        return
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            print("\nProposed Analysis:")
            for column, dtype in data.items():
                if column in dataframe.columns:
                    print(f" - {column}: {dtype}")
                    if 'date' in column.lower() or 'time' in column.lower():
                        print("   (This column appears to be a date/time field.)")
                        dataframe[column] = pd.to_datetime(dataframe[column], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        print("\nDataFrame after applying proposed analysis:")
        print(dataframe.info())
        print(dataframe.head())
        return dataframe
    except Exception as e:
        print(f"Error reading {json_file}: {e}")

# Function 5: Save to DuckDB
def save_df_tofile(dataframe: pd.DataFrame, parameters: dict):
    try:
        with duckdb.connect(ANALYZED_DB) as con:
            columns = dataframe.columns.tolist()
            column_definitions = []
            for col in columns:
                dtype = dataframe[col].dtype
                if dtype in ['int64', 'int32']:
                    column_definitions.append(f"{col} INTEGER")
                elif dtype in ['float64', 'float32']:
                    column_definitions.append(f"{col} FLOAT")
                elif dtype == 'datetime64[ns]':
                    column_definitions.append(f"{col} TIMESTAMP")
                elif dtype == 'bool':
                    column_definitions.append(f"{col} BOOLEAN")
                else:
                    column_definitions.append(f"{col} VARCHAR")
            con.execute(f"CREATE OR REPLACE TABLE {ANALYZED_TABLE} ({', '.join(column_definitions)})")
            data_tuples = [tuple(row.tolist()) for _, row in dataframe.iterrows()]
            placeholders = ', '.join(['?' for _ in columns])
            con.executemany(f"INSERT INTO {ANALYZED_TABLE} ({', '.join(columns)}) VALUES ({placeholders})", data_tuples)
    except Exception as e:
        print(f"Error saving DataFrame to database: {e}")

# Parameter Management Functions
def load_parameters(param_file):
    params = {}
    if not os.path.exists(param_file):
        print(f"Parameter file {param_file} not found. Creating new database...")
        create_parameter_db(param_file)
        return params
    try:
        with duckdb.connect(param_file, read_only=True) as con:
            rows = con.execute("SELECT key, value, alternative_key, alternative_value FROM parameters").fetchall()
            for row in rows:
                params[row[0]] = {
                    "key": row[0],
                    'value': row[1],
                    'alternative_key': row[2],
                    'alternative_value': row[3]
                }
    except Exception as e:
        print(f"Error loading parameters: {e}")
    return params

def save_parameters(param_file, params):
    with duckdb.connect(param_file) as con:
        con.execute("CREATE TABLE IF NOT EXISTS parameters (key VARCHAR, value VARCHAR, alternative_key VARCHAR, alternative_value VARCHAR)")
        con.execute("DELETE FROM parameters")
        for key, value in params.items():
            con.execute(
                "INSERT INTO parameters (key, value, alternative_key, alternative_value) VALUES (?, ?, ?, ?)",
                [value.get('key', key), value.get('value', ''), value.get('alternative_key', ''), value.get('alternative_value', '')]
            )

def get_similar_columns_chatgpt(paramlist: list, columnlist: list) -> str:
    """Analyze CSV data and compare with saved parameters using ChatGPT API."""
    url = "https://models.github.ai/inference/chat/completions"
    prompt = f"""Generate a JSON object that maps Column headers: {columnlist} with similar descriptions from the parameters: {paramlist}. 
    
    Return only a valid JSON object in this format:
    {{
        "column_name_1": "matching_parameter_description",
        "column_name_2": "matching_parameter_description"
    }}
    
    Do not include any other text or explanation, just the JSON object."""
    try:
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        data = {
            "model": "DeepSeek-V3-0324",
            "messages": [{"role": "user", "content": prompt}]
        }
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                content = result['choices'][0]['message']['content']
                if content.startswith('```json'):
                    content = content.replace('```json', '').replace('```', '').strip()
                elif content.startswith('```'):
                    content = content.replace('```', '').strip()
                return content
            else:
                return "No response content found"
        else:
            print(f"API request failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return "API request failed"
    except Exception as e:
        print(f"Error calling ChatGPT API: {e}")

def create_parameter_db(param_file):
    """Create a parameter database if it doesn't exist"""
    if not os.path.exists(param_file):
        with duckdb.connect(param_file) as con:
            con.execute("""
                CREATE TABLE parameters (
                    key VARCHAR,
                    value VARCHAR,
                    alternative_key VARCHAR,
                    alternative_value VARCHAR
                )
            """)
            print("Parameter database created.")

def add_country_codes(cc: dict[str, str]) -> None:
    """Add country codes to parameters"""
    params = load_parameters(PARAM_DB)
    for code, country in cc.items():
        params[code] = {
            "key": f"countries_{code}",
            "value": "character 2",
            "alternative_key": code,
            "alternative_value": country
        }
    save_parameters(PARAM_DB, params)

def add_static_parameters() -> dict:
    """Add static parameters to database"""
    params = load_parameters(PARAM_DB)
    day_numbers = list(range(1, 8))
    day_names = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']
    df_days = pd.DataFrame({
        'day_of_week': day_numbers,
        'day_of_week_text': day_names,
        'day_of_week_sin': np.sin(2 * np.pi * np.array(day_numbers) / 7),
        'day_of_week_cos': np.cos(2 * np.pi * np.array(day_numbers) / 7)
    })
    month_numbers = list(range(1, 13))
    month_names = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                   'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    df_months = pd.DataFrame({
        'month': month_numbers,
        'month_text': month_names,
        'month_sin': np.sin(2 * np.pi * np.array(month_numbers) / 12),
        'month_cos': np.cos(2 * np.pi * np.array(month_numbers) / 12)
    })
    df = pd.concat([df_days, df_months], axis=1)
    for column in df.columns:
        params[column] = {
            "key": column,
            "value": df[column].dropna().tolist(),
            "alternative_key": "",
            "alternative_value": ""
        }
    save_parameters(PARAM_DB, params)
    return params

# Main Function
def main(country_codes: dict):
    print("Preloading parameter file...")
    params = load_parameters(PARAM_DB)
    print("Parameter file loaded successfully.")

    df = None
    while True:
        print("""=== CSV Data Analyzer ===
            1️⃣  Load CSV File for Analysis
            2️⃣  Manual Analysis
            3️⃣  ChatGPT Analysis
            4️⃣  Display Analysis
            5️⃣  Save to DuckDB
            6️⃣  Pack a CSV
            7️⃣  Unpack a CSV
            40  Converting BIG CSV into chunks
            4️⃣1️⃣  Display Parameters
            4️⃣2️⃣  Add Country Codes
            4️⃣3️⃣  Add Static Data Parameters
            4️⃣4️⃣  Create Parameter File
            4️⃣5️⃣  Edit a Parameter
            0️⃣  Exit
          """)
        choice = input("Select an option (1-45): ").strip()
        if choice == '1':
            df = load_csv_file()
        elif choice == '2':
            if df is None:
                print("Please load a CSV file first using option 1.")
                continue
            manual_analysis(dataframe=df, parameters=params)
        elif choice == '3':
            if df is None:
                print("Please load a CSV file first using option 1.")
                continue
            analyze_csv(dataframe=df, params=params)
        elif choice == '4':
            if df is None:
                print("Please load a CSV file first using option 1.")
                continue
            display_proposed_analysis(dataframe=df, json_file=CHATGPT_RESULTS)
        elif choice == '5':
            if df is None:
                print("No data loaded. Please load a CSV file first using option 1.")
                continue
            save_df_tofile(dataframe=df, parameters=params)
        elif choice == '6':
            try:
                import zlib
                csv_files = [f for f in os.listdir('src') if f.endswith('.csv')]
                if not csv_files:
                    print("No CSV files found in the 'src' directory.")
                    return
                for i, file in enumerate(csv_files):
                    print(f"{i+1}. {file}")
                print(f"{len(csv_files)+1}. Exit")

                choice_file = input(f"Select a file (1-{len(csv_files)+1}): ").strip()
                if choice_file.isdigit():
                    index = int(choice_file) - 1
                    if index == len(csv_files):
                        print("Exiting.")
                        return
                    if 0 <= index < len(csv_files):
                        file_to_pack = os.path.join('src', csv_files[index])
                        with open(file_to_pack, 'rb') as f:
                            packed = zlib.compress(f.read())
                        packed_file = file_to_pack.replace('.csv', '') + '.zlib'
                        with open(packed_file, 'wb') as f:
                            f.write(packed)
                        print(f"✅ Packed file saved as {packed_file}")
                    else:
                        print("Invalid choice.")
                else:
                    print("Invalid input.")
            except Exception as e:
                print(f"❌ Error packing CSV: {e}")

        elif choice == '7':
            try:
                import zlib
                zlib_files = [f for f in os.listdir('src') if f.endswith('.zlib')]
                if not zlib_files:
                    print("No zlib files found in the 'src' directory.")
                    return
                for i, file in enumerate(zlib_files):
                    print(f"{i+1}. {file}")
                print(f"{len(zlib_files)+1}. Exit")

                choice_file = input(f"Select a file (1-{len(zlib_files)+1}): ").strip()
                if choice_file.isdigit():
                    index = int(choice_file) - 1
                    if index == len(zlib_files):
                        print("Exiting.")
                        return
                    if 0 <= index < len(zlib_files):
                        file_to_unpack = os.path.join('src', zlib_files[index])
                        with open(file_to_unpack, 'rb') as f:
                            compressed = f.read()
                        decompressed = zlib.decompress(compressed).decode("utf-8")
                        unpacked_file = file_to_unpack.replace('.zlib', '.csv')
                        with open(unpacked_file, 'w') as f:
                            f.write(decompressed)
                        print(f"✅ Unpacked file saved as {unpacked_file}")
                    else:
                        print("Invalid choice.")
                else:
                    print("Invalid input.")
            except Exception as e:
                print(f"❌ Error unpacking zlib file: {e}")
        elif choice == '41':
            print("Loaded Parameters:")
            for key, value in params.items():
                print(f"{key}: {value}")
        elif choice == '42':
            add_country_codes(country_codes)
        elif choice == '43':
            add_static_parameters()
        elif choice == '44':
            create_parameter_db(PARAM_DB)
        elif choice == '45':
            key_to_edit = input("Enter the parameter key to edit: ").strip()
            if key_to_edit in params:
                print(f"Current value for '{key_to_edit}': {params[key_to_edit]}")
                new_value = input(f"Enter new value for '{key_to_edit}' (leave blank to keep current value): ").strip()
                new_alt_key = input(f"Enter new alternative key for '{key_to_edit}' (leave blank to keep current value): ").strip()
                new_alt_value = input(f"Enter new alternative value for '{key_to_edit}' (leave blank to keep current value): ").strip()
                if new_value:
                    params[key_to_edit]['value'] = new_value
                if new_alt_key:
                    params[key_to_edit]['alternative_key'] = new_alt_key
                if new_alt_value:
                    params[key_to_edit]['alternative_value'] = new_alt_value
                save_parameters(PARAM_DB, params)
                print(f"Parameter '{key_to_edit}' updated successfully.")
            else:
                print(f"Parameter '{key_to_edit}' not found.")
        elif choice == '0':
            print("Exiting...")
            break

if __name__ == "__main__":
    main(country_codes={})
