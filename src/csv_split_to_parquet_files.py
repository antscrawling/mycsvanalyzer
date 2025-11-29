import duckdb
import glob
import os
from pathlib import Path
from tqdm import tqdm

# Define the root directory for the project
OUTPUT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_FILES = os.path.join(OUTPUT_ROOT, 'src')

# Define output directories for split files
SPLIT_CSV_FILES = 'SPLITCSV'
SPLIT_PARQUET_FILES = 'SPLITPARQUET'
SPLIT_ZLIB_FILES = 'SPLITZLIB'

SPLIT_CSV_PATH = os.path.join(SRC_FILES, SPLIT_CSV_FILES)
SPLIT_PARQUET_PATH = os.path.join(SRC_FILES, SPLIT_PARQUET_FILES)
SPLIT_ZLIB_PATH = os.path.join(SRC_FILES, SPLIT_ZLIB_FILES)

CSV_FILE_EXTENSION = 'csv' 
PARQUET_FILE_EXTENSION = 'parquet'
ZLIB_FILE_EXTENSION = 'zlib'


def rebuild_parquet_file():
    """
    Rebuild to a SINGLE CSV File from multiple Parquet chunks in a directory.
    
    Args:
        parquet_dir (str): Directory containing the Parquet chunk files.
        output_file (str): Path to the output combined Parquet file.
    """
    # Find all parquet parts
    parquet_files = glob.glob(os.path.join(SPLIT_PARQUET_PATH, "*.parquet"))
    # GET A SINGLE FILE AND SAVE THE FILENAME AND REMOIVE "_x.parquet"
    parquet_file = F"{SRC_FILES}/{parquet_file}" # GET THE Filename without dot and extension 
    
    #OUTPUT OF THE SAVED parquet will be SRC_FILES/{parquet_file}.csv
    # Read and concat
    # Use DuckDB to read and combine all parquet files
    df = duckdb.read_parquet(parquet_files).df()

    # Save back as single CSV FILE
    df.to_csv(parquet_file)

def save_to_parquet_file(input_file,chunk_size=10000):
    """
    Save a large CSV file to Parquet format in smaller chunks

    Args:
        input_file (str): Path to the input CSV file
        chunk_size (int): Number of rows per chunk (default: 10000)
        output_dir (str): SPLITPARQUET
       

    Returns:
        dict: Summary of the save operation
    """

    print("📁 BIG CSV TO PARQUET CONVERTER")
    print("=" * 30)
    input_files = SPLIT_PARQUET_PATH
    
    # Validate input file
    if not os.path.exists(input_files):
        print(f"❌ Error: File '{input_files}' not found!")
        return None

    try:
        # First, count total rows for progress bar
        print("\n🔍 Analyzing file structure...")
        total_rows = sum(1 for _ in open(input_file)) - 1  # Subtract header
        total_chunks = (total_rows + chunk_size - 1) // chunk_size  # Ceiling division

        print(f"📊 Total data rows: {total_rows:,}")
        print(f"📦 Expected chunks: {total_chunks}")

        # Read and save the CSV file to Parquet in chunks
        print("\n⚡ Processing chunks...")

        chunk_info = []

        # Use tqdm for progress tracking
        chunk_iterator = pd.read_csv(input_file, chunksize=chunk_size)

        for i, chunk in enumerate(tqdm(chunk_iterator, total=total_chunks, desc="Saving to Parquet")):
            chunk_filename = f"{input_file}_{i + 1:03d}.parquet"  # Zero-padded numbering
            chunk_path = os.path.join(SPLIT_PARQUET_PATH, chunk_filename)

            # Save chunk to Parquet
            chunk.to_parquet(chunk_path, index=False)

            # Store chunk info
            chunk_info.append({
                'chunk_number': i + 1,
                'filename': chunk_filename,
                'rows': len(chunk),
                'size_mb': os.path.getsize(chunk_path) / (1024 * 1024)
            })

        # Summary
        print("\n✅ Parquet chunks saved successfully!")
        return {
            'input_file': input_file,
            'output_dir': SPLIT_PARQUET_PATH,
            'total_chunks': len(chunk_info),
            'chunk_info': chunk_info
        }

    except Exception as e:
        print(f"❌ Error saving to Parquet: {e}")
        return None

def split_csv_to_parquet(input_file, chunk_size=10000, output_dir=SPLIT_PARQUET_PATH):
    """
    Split a large CSV file into smaller Parquet files.

    Args:
        input_file (str): Path to the input CSV file
        chunk_size (int): Number of rows per chunk (default: 10000)
        output_dir (str): Directory to save Parquet chunks

    Returns:
        dict: Summary of the split operation
    """
    base_name = os.path.splitext(os.path.basename(input_file))[0]

    if not os.path.exists(input_file):
        print(f"❌ Error: File '{input_file}' not found!")
        return None

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    try:
        total_rows = sum(1 for _ in open(input_file)) - 1  # Subtract header
        total_chunks = (total_rows + chunk_size - 1) // chunk_size

        chunk_iterator = pd.read_csv(input_file, chunksize=chunk_size)
        for i, chunk in enumerate(tqdm(chunk_iterator, total=total_chunks, desc="Splitting to Parquet")):
            chunk_filename = f"{base_name}_part{i + 1:03d}.parquet"
            chunk_path = os.path.join(output_dir, chunk_filename)
            chunk.to_parquet(chunk_path, index=False)

        print("✅ Parquet files created successfully.")
    except Exception as e:
        print(f"❌ Error: {e}")

def rebuild_parquet_files(input_dir, output_file):
    """
    Rebuild multiple Parquet files into a single CSV file.

    Args:
        input_dir (str): Directory containing Parquet files
        output_file (str): Path to the output CSV file

    Returns:
        None
    """
    if not os.path.exists(input_dir):
        print(f"❌ Error: Directory '{input_dir}' not found!")
        return

    try:
        # List all Parquet files in the directory
        parquet_files = [f for f in sorted(os.listdir(input_dir)) if f.endswith(PARQUET_FILE_EXTENSION)]
        if not parquet_files:
            print("❌ No Parquet files found in the directory.")
            return

        # Open the output CSV file for writing
        with open(output_file, 'w') as outfile:
            for i, file in enumerate(parquet_files):
                file_path = os.path.join(input_dir, file)

                # Read the Parquet file into a DataFrame
                chunk = pd.read_parquet(file_path)

                # Write the DataFrame to the CSV file
                chunk.to_csv(outfile, index=False, header=(i == 0))  # Write header only for the first file

        print(f"✅ Successfully rebuilt CSV file: {output_file}")
    except Exception as e:
        print(f"❌ Error rebuilding CSV file: {e}")

if __name__ == "__main__":
    # Example usage
    input_csv = os.path.join(SRC_FILES, 'example.csv')
    split_csv_to_parquet(input_csv)

    input_parquet_dir = SPLIT_PARQUET_PATH
    output_csv = os.path.join(SRC_FILES, 'rebuilt.csv')
    rebuild_parquet_files(input_parquet_dir, output_csv)

