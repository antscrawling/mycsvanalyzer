import os
import sys
from pathlib import Path
from tqdm import tqdm
import argparse
import shutil
import json
import zlib
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import os,sys

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

# Define output directories for split files
SPLIT_CSV_PATH = SPLIT_CSV_FILES  # Output CSV files in csvanalyzer/src/SPLITCSV
PARQUET_PATH = os.path.join(SRC_FILES, PARQUET_FILES)  # Output Parquet files in csvanalyzer/src/PARQUET
ZLIB_PATH = os.path.join(SRC_FILES, ZLIB_FILES)  # Output Zlib files in csvanalyzer/src/ZLIB

def list_csv_files():
    """List available CSV files in the src directory with an option to exit"""
    src_directory = SRC_FILES  # Ensure it points to 'csvanalyzer/src'

    # Check if the directory exists
    if not os.path.exists(src_directory):
        print(f"❌ Error: Source directory '{src_directory}' does not exist!")
        return [], src_directory

    # List CSV files in the directory
    csv_files = [f for f in os.listdir(src_directory) if f.endswith('.csv')]

    if csv_files:
        print("\n📁 Available CSV files in src directory:")
        for i, file in enumerate(csv_files, 1):
            size_mb = os.path.getsize(os.path.join(src_directory, file)) / (1024 * 1024)
            print(f"   {i}. {file} ({size_mb:.2f} MB)")
        print("   0. Exit")  # Add an explicit exit option
        return csv_files, src_directory
    else:
        print("\n📁 No CSV files found in src directory")
        return [], src_directory

def split_csv_file(file_type:str):
    """Interactive mode for user-friendly file selection"""
    print("🔄 INTERACTIVE MODE")
    print("=" * 20)

    # List available files
    csv_files, src_directory = list_csv_files()

    # BIG CSV IS IN CSVANALYZER/SRC
    # SPLIT CSV FILES GO TO CSVANALYZER/SRC/SPLITCSV
    if not csv_files:
        print("❌ No CSV files found. Please add a CSV file to the src directory.")
        return

    # Let user select file
    while True:
        try:
            choice = input(f"\nSelect a file (1-{len(csv_files)}) or enter filename (or type 'exit' to quit): ").strip()

            if choice.lower() == 'exit':
                print("👋 Exiting interactive mode.")
                return

            if choice == '0':
                print("👋 Exiting interactive mode.")
                return

            if choice.isdigit():
                index = int(choice) - 1
                if 0 <= index < len(csv_files):
                    csv_choice_file = csv_files[index]
                    break
                else:
                    print(f"❌ Invalid choice. Please select 1-{len(csv_files)}")
            elif os.path.exists(os.path.join(SRC_FILES, choice)):
                csv_choice_file = choice
                break
            else:
                print(f"❌ File '{choice}' not found in src directory")
        except KeyboardInterrupt:
            print("\n👋 Cancelled by user")
            return

    # Get chunk size
    while True:
        try:
            chunk_input = input("\nChunk size (rows per file) [default: 10000]: ").strip()
            if not chunk_input:
                chunk_size = 10000
                break
            else:
                chunk_size = int(chunk_input)
                if chunk_size > 0:
                    break
                else:
                    print("❌ Chunk size must be positive")
        except ValueError:
            print("❌ Please enter a valid number")
        except KeyboardInterrupt:
            print("\n👋 Cancelled by user")
            return

    # Get output directory
    output_dir = input("\nOutput directory [default: SPLITCSV]: ").strip()
    # if the user entered a folder name, save this 
    if output_dir is None:
        output_dir = SPLIT_CSV_FOLDER
    else :
        # Ensure output_dir is properly initialized
        if not output_dir:
            output_dir = SPLIT_CSV_FILES

        # Create the directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    # Confirm operation
    print("\n📋 OPERATION SUMMARY:")
    print(f"   📄 Input file: {csv_choice_file}")
    print(f"   📦 Chunk size: {chunk_size:,} rows")
    print(f"   📁 Output directory: {output_dir}")
    print(f"   📏 File size: {os.path.getsize(os.path.join(SRC_FILES, csv_choice_file)) / (1024 * 1024):.2f} MB")

    confirm = input("\nProceed? (y/N): ").strip().lower()
    if confirm == 'y':
        
        mysplitdict = {"output_dir":output_dir,"csv_choice_file":csv_choice_file,"chunk_size":chunk_size}
        file = "csv_split.json"
        # Move JSON saving logic after successful file processing
        result = process_csv_file(output_dir, csv_choice_file, chunk_size)
        if result:
            with open(file, 'w') as f:
                json.dump(mysplitdict, f, indent=4)
            print("\n🎉 Operation completed successfully!")
    else:
        print("❌ Operation cancelled")
def process_csv_file(output_dir,csv_choice_file, chunk_size):
    """
    Split a large CSV file into smaller chunks with headers

    Args:
        csv_choice_file (str): Path to the input CSV file
        chunk_size (int): Number of rows per chunk (default: 10000)
        output_dir (str): Directory to save the split files

    Returns:
        dict: Summary of the split operation
    """
    # Create output directory if it doesn't exist
   
    file_type = 'CSV'  # Default to CSV
    with open(f"{SRC_FILES}/{csv_choice_file}", 'r') as infile:
        # Read the header
        header = infile.readline()

        chunk_index = 0
        rows = []
        for i, line in enumerate(infile):
            rows.append(line)
            # If we have enough rows, or it's the last chunk, write to file
            if len(rows) >= chunk_size or (i + 1) == csv_choice_file:
                chunk_index += 1
                # Determine output file path
                # a = csv_choice_file = sample.csv
                # b = a.replace('.csv',"") = sample
                # c = f"{b}_part{chunk_index}.csv" = sample_part1.csv
                if file_type == 'CSV':
                    base_name = os.path.basename(csv_choice_file).replace('.csv', '')
                    output_file = os.path.join(output_dir, f"{base_name}_part{chunk_index}.csv")
                else:
                    print(f"❌ Unknown file type: {file_type}")
                    return

                # Write the chunk to the appropriate file
                if file_type == 'CSV':
                    with open(output_file, 'w') as chunk_file:
                        chunk_file.write(header)  # Write the header
                        chunk_file.writelines(rows)  # Write the chunk rows
                elif file_type == 'PARQUET':
                    # Ensure pyarrow is imported as `pa`
                    import pyarrow as pa
                    table = pa.Table.from_pandas(pd.DataFrame([row.strip().split(',') for row in rows]))
                    pq.write_table(table, output_file)
                elif file_type == 'ZLIB':
                    with open(output_file, 'wb') as chunk_file:
                        chunk_file.write(zlib.compress(''.join(rows).encode()))
                
                print(f"✅ Created: {output_file} ({len(rows)} rows)")
                rows = []  # Reset rows for the next chunk

    return {
        'input_file': csv_choice_file,
        'output_dir': output_dir,
        'chunk_size': chunk_size,
        'total_chunks': chunk_index
    }
    

def rebuild_csv_files():
    """
    Rebuild split CSV files from SPLITCSV directory into a single CSV file.

    The function assumes that the split files are named in the format <base_name>_partX.csv
    and combines them into a single CSV file in the SRC_FILES directory.
    """
    # scan the directory with a folder having SPLIT_<name>
    # check that there are files with <file>_part1.csv  <file>_part2.csv etc.


    if not os.path.exists(f"{SRC_FILES}/csv_split.json"):
        raise FileNotFoundError("Standard folder SPLITCSV not found")
        return
    
        # get the files
    
    with open(f"{SRC_FILES}/csv_split.json", 'r') as f:
        mysplitdict = json.load(f)
    split_dir = mysplitdict["output_dir"]
    output_dir = SRC_FILES
    output_file = mysplitdict["csv_choice_file"]
   # chunk_size = mysplitdict["chunk_size"]
    csv_files = [ f for f in os.listdir(split_dir) if f.endswith(CSV_FILE_EXTENSION)]
    # get the csv file sample_part1.csv and 
        
    try:
        # List all CSV files in the split directory
        csv_files = [f for f in sorted(SPLIT_CSV_PATH) if f.endswith(CSV_FILE_EXTENSION)]

        print("\n📋 REBUILD OPERATION SUMMARY:")
        print(f"   📁 Split directory: {split_dir}")
        print(f"   📄 Files to rebuild: {len(csv_files)}")
        print(f"   📂 Output file: {output_dir}{output_file}")

        confirm = input("\nProceed with rebuilding? (y/N): ").strip().lower()
        if confirm != 'y':
            print("❌ Operation cancelled.")
            return

        # Combine all CSV files into a single file
        with open(output_file, 'w') as outfile:
            for i, file in enumerate(csv_files):
                file_path = os.path.join(split_dir, file)
                with open(file_path, 'r') as infile:
                    if i == 0:
                        # Write header for the first file
                        outfile.write(infile.read())
                    else:
                        # DO NOT SKIP HEADER Skip header for subsequent files
                    
                        next(infile)
                        outfile.write(infile.read())

        print(f"✅ Successfully rebuilt CSV file: {output_file}")
    except Exception as e:
        print(f"❌ Error rebuilding CSV file: {e}")


























































def main():
    #rebuild_csv_files()
    split_csv_file('CSV')
    print(f"The output root is {OUTPUT_ROOT}")
    print(f"The source files are in {SRC_FILES}")
    print(f"The big csv files are in {BIG_CSV_FILES}    ")
    print(f"The split CSV files are in {SPLIT_CSV_PATH}")
    print(f"The split Parquet files are in {PARQUET_PATH}")
    print(f"The split Zlib files are in {ZLIB_PATH}")

if __name__ == "__main__":
  
   main()
   #rebuild_csv_files()
