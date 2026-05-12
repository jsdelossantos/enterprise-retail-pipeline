import pandas as pd
from pathlib import Path
from db_connect import get_db_connection
import psycopg2.extras

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

    # Replaces all NaN values with Python's None
    sellers_df = sellers_df.where(pd.notnull(sellers_df), None)

    return sellers_df


def load_sellers_data_to_postgres():
    
    # Connect to db first
    conn = get_db_connection()
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        current_script_path = Path(__file__).resolve()
        
        # Get root of sql file for creation of table
        project_root = current_script_path.parent.parent
        sql_file = project_root / "sql" / "01_create_sellers_table.sql"

        # Read content of SQL file to be put onto query variable then execute it
        print(f"Reading SQL file from: {sql_file}")
        with open(sql_file, "r") as file:
            sql_query = file.read()
        
        cursor.execute(sql_query)
        print("Table successfully created or verified in PostgreSQL!")
        conn.commit()

        # Get the seller data set as df, then turn it into tuple for bulk insertion of data onto table
        sellers_df = extract_sellers_data()
        sellers_data = list(sellers_df.itertuples(index=False, name=None))

        insert_query = """
            INSERT INTO stg_sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
            VALUES %s;
        """

        print("Pushing data to Postgresql")
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            sellers_data
        )

        conn.commit()
        print(f"Success! {len(sellers_data)} rows loaded into the sellers table.")


    except Exception as e:
        print(f"Failed to load data. Error {e}")
        conn.rollback()
    
    finally:
        if conn is not None:
            cursor.close()
            conn.close()



if __name__ == "__main__":
    load_sellers_data_to_postgres()
