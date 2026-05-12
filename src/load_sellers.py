import pandas as pd
from pathlib import Path

def extract_sellers_data():
    # getting the path of the data
    current_script_path = Path(__file__).resolve()
    project_root = current_script_path.parent.parent

    csv_file = project_root / "data" / "olist_sellers_dataset.csv"

    print(f"Attempting to read file from: {csv_file}")
    try:
        sellers_df = pd.read_csv(csv_file, encoding='utf-8')

    except FileNotFoundError:
        print("Error: Could not find the csv file")
        return None
    
    print("\n--- Data Extraction Successful ---")

    print("\nPreview of the data:")
    print(sellers_df.head())

    print("\nSchema Summary:")
    print(sellers_df.info())

    return sellers_df


if __name__ == "__main__":
    extract_sellers_data()
