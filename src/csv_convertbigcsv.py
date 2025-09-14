#!/usr/bin/env python3
"""
Big CSV File Splitter
Splits large CSV files into smaller chunks while preserving headers
Each chunk file is named: chunk_1.csv, chunk_2.csv, etc.
"""

import pandas as pd
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
from csv_split_to_csv_files import list_csv_files, split_csv_file, rebuild_csv_files

# directory is csvanalyzer
OUTPUT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # Ensure OUTPUT_ROOT points to 'csvanalyzer' folder

# src files are in csvanalyzer/src
SRC_FILES = os.path.join(OUTPUT_ROOT, 'src')

# below folders are in csvanalyzer/src/*
SPLIT_CSV_FILES = 'SPLITCSV'
SPLIT_PARQUET_FILES = 'SPLITPARQUET'
SPLIT_ZLIB_FILES = 'SPLITZLIB'

# Define output directories for split files
SPLIT_CSV_PATH = os.path.join(SRC_FILES, SPLIT_CSV_FILES)  # Output CSV files in csvanalyzer/src/SPLITCSV
SPLIT_PARQUET_PATH = os.path.join(SRC_FILES, SPLIT_PARQUET_FILES)  # Output Parquet files in csvanalyzer/src/SPLITPARQUET
SPLIT_ZLIB_PATH = os.path.join(SRC_FILES, SPLIT_ZLIB_FILES)  # Output Zlib files in csvanalyzer/src/SPLITZLIB

# Ensure directories exist
os.makedirs(SPLIT_CSV_PATH, exist_ok=True)
os.makedirs(SPLIT_PARQUET_PATH, exist_ok=True)
os.makedirs(SPLIT_ZLIB_PATH, exist_ok=True)

INPUT_FILES = SRC_FILES
# 

def main():
    """Main function with menu-driven options"""
    while True:
        print("\n📋 MENU")
        print("1. Split CSV into chunks of CSV")
        print("2. Split CSV into chunks of Parquet")
        print("3. Split CSV into chunks of Zlib")
        print("4. Rebuild split CSV into one CSV")
        print("5. Rebuild split Parquet into one Parquet")
        print("6. Rebuild split Zlib into one Zlib")
        print("0. Exit")

        choice = input("\nEnter your choice: ").strip()

        if choice == "1":
            # Call the function to split CSV into chunks of CSV
            split_csv_file('CSV')
        elif choice == "2":
            # Call the function to split CSV into chunks of Parquet
            print("Feature for splitting CSV into Parquet chunks is under development.")
            #split_csv_menu('PARQUET')
        elif choice == "3":
            # Call the function to split CSV into chunks of Zlib
            print("Feature for splitting CSV into Zlib chunks is under development.")
            #split_csv_file('ZLIB')
        elif choice == "4":
            # Call the function to rebuild split CSV into one CSV
            # this is unspli4t
            rebuild_csv_files()
            #print("Feature for rebuilding split CSV is under development.")
        elif choice == "5":
            # Call the function to rebuild split Parquet into one Parquet
            print("Feature for rebuilding split Parquet is under development.")
        elif choice == "6":
            # Call the function to rebuild split Zlib into one Zlib
            print("Feature for rebuilding split Zlib is under development.")
        elif choice == "0":
            print("👋 Goodbye!")
            break
        else:
            print("❌ Invalid choice. Please try again.")

# Rearrange functions to match the menu order
# 1. list_csv_files
# 2. interactive_mode
# 3. split_csv_file
# 4. save_to_parquet_in_chunks
# 5. quick_split
# 6. main

if __name__ == "__main__":
    main()
