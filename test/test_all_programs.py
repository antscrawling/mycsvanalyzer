import os
import subprocess

def run_program(program_path):
    try:
        print(f"Running {program_path}...")
        result = subprocess.run(["python", program_path], check=True, capture_output=True, text=True)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error while running {program_path}: {e.stderr}")

if __name__ == "__main__":
    # List of retail menu programs to test
    retail_menu_programs = [
        "src/main.py",
        "src/retail_menu.py",
        "src/retail_analysis.py",
        "src/hourly_analysis.py",
        "src/hourly_discount_analysis.py",
        "src/hourly_sales_analysis.py",
    ]

    # List of CSV analyzer programs to test
    csv_analyzer_programs = [
        "src/analyze_csv.py",
        "src/convertbigcsv.py",
        "src/pack_csv.py",
        "src/unpack_csv.py",
    ]

    # Run all retail menu programs
    for program in retail_menu_programs:
        run_program(program)

    # Run all CSV analyzer programs
    for program in csv_analyzer_programs:
        run_program(program)
