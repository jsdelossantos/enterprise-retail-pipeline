import pandas as pd
from pathlib import Path
from db_connect import get_db_connection
import psycopg2.extras
from psycopg2 import sql

def extract_data(dataset_name):
    # getting the path of the data
    current_script_path = Path(__file__).resolve()
    project_root = current_script_path.parent.parent

    csv_file = project_root / "data" / dataset_name

    print(f"Attempting to read file from: {csv_file}")
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')

    except FileNotFoundError:
        print("Error: Could not find the csv file")
        return None
    
    print("\n--- Data Extraction Successful ---")

    print("\nPreview of the data:")
    print(df.head())

    print("\nSchema Summary:")
    print(df.info())

    # Replaces all NaN values with Python's None
    df = df.where(pd.notnull(df), None)

    return df


def execute_sql_file(conn, sql_file_path):
    
    # Connect to db first
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()
        current_script_path = Path(__file__).resolve()
        
        # Get root of sql file for creation of table
        project_root = current_script_path.parent.parent
        sql_file = project_root / "sql" / sql_file_path

        # Read content of SQL file to be put onto query variable then execute it
        print(f"Reading SQL file from: {sql_file}")
        with open(sql_file, "r") as file:
            sql_query = file.read()
        
        cursor.execute(sql_query)
        print("Table successfully created or verified in PostgreSQL!")
        conn.commit()

    except Exception as e:
        print(f"Failed to create table. Error {e}")
        conn.rollback()
    
    # we do not close it inside the function because it would interfere with the other functions if it closed beforehand. connection should be closed outside.
    finally:
        if conn is not None:
            cursor.close()


def bulk_insert_dataframe(conn, df, table_name):
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()

        # Convert df into tuple for bulk insertion of data onto table
        data = list(df.itertuples(index=False, name=None))

        # gets the columns so the query wouldn't be static
        columns_list = list(df.columns)
        
        # convert every string in the list into a safe identifier object
        safe_columns = [sql.Identifier(col) for col in columns_list]

        insert_query = sql.SQL("""
            INSERT INTO {table} ({cols})
            VALUES %s;
        """).format(
            table=sql.Identifier(table_name),
            cols=sql.SQL(", ").join(safe_columns)
        )

        # psycopg2 takes the cursor, the query, and then the data to be inserted
        print("Pushing data to Postgresql")
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            data
        )

        conn.commit()
        print(f"Success! {len(data)} rows loaded into the {table_name} table.")


    except Exception as e:
        print(f"Failed to load data. Error {e}")
        conn.rollback()
    
    # we do not close it inside the function because it would interfere with the other functions if it closed beforehand. connection should be closed outside.
    finally:
        if conn is not None:
            cursor.close()



if __name__ == "__main__":
    conn = get_db_connection()
    
    # 1. Extract
    df = extract_data("olist_sellers_dataset.csv")
    
    # 2. Create Table (Staging)
    execute_sql_file(conn, "01_create_sellers_table.sql")
    
    # 3. Load Data
    bulk_insert_dataframe(conn, df, "stg_sellers")

    conn.close()
