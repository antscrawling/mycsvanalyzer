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
        "src/api.py"
    ]

  


    # Run all retail menu programs
    for program in retail_menu_programs:
        run_program(program)

    # Run all CSV analyzer programs
    for program in csv_analyzer_programs:
        run_program(program)
