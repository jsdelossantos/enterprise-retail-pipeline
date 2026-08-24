import pandas as pd
from pathlib import Path
from db_connect import get_db_connection
import psycopg2.extras
from psycopg2 import sql
import logging
import json

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


def bulk_insert_dataframe(conn, df, table_name, primary_key=None):
    if conn is None:
        return
    
    try:
        cursor = conn.cursor()

        # Convert df into tuple for bulk insertion of data onto table
        data = list(df.itertuples(index=False, name=None))

        # gets the columns so the query wouldn't be static
        columns_list = list(df.columns)

        # if the table has a primary key, upsert
        if primary_key:
            # need to add a pk_logic to determine if need to use a composite key or a primary key
            if isinstance(primary_key, list):
                pk_logic = sql.SQL(", ").join([sql.Identifier(col) for col in primary_key])
            else:
                pk_logic = sql.Identifier(primary_key)

                # converting string to list for getting update_columns
                primary_key = [primary_key]

            # create a update_column for upserting data
            update_columns = [col for col in columns_list if col not in primary_key]

            # create an empty list to hold our "safe" psycopg2 objects
            set_fragments = []

            # We loop through the columns one by one
            for col in update_columns:
                
                # Put the column name into an "Identifier" box so it is safe
                safe_col = sql.Identifier(col)
                
                # Create the command blueprint: {col} = EXCLUDED.{col}
                # Then use .format() to plug our safe_col into the {col} slots
                # this fragment query would dynamically generate this example: "order_status" = EXCLUDED."order_status"
                # and it would do it per column fro the loop
                fragment = sql.SQL("{col} = EXCLUDED.{col}").format(col=safe_col)
                
                # Add this finished fragment to our list
                set_fragments.append(fragment)

            # Finally, we take our list of fragments and glue them together with commas.
            # sql.SQL(", ") is the glue. .join() snaps them all together.
            final_set_logic = sql.SQL(", ").join(set_fragments)

            # a list comprehension, basically does the same thing in the loop above
            # convert every string in the list into a safe identifier object
            safe_columns = [sql.Identifier(col) for col in columns_list]

            # so in the insert query, in using psycopg2, we pretty much need to 'contain' the code in their corresponding 'types', at least that's how i understand it
            # for sql queries, we use sql.SQL and inside it the query itself, the variables inside {} are dynamic, and we use format to 'identify' what they are
            insert_query = sql.SQL("""
                        INSERT INTO {table} ({cols})
                        VALUES %s
                        ON CONFLICT ({pk}) 
                        DO UPDATE SET {set_logic};
                    """).format(
                        table=sql.Identifier(table_name),
                        cols=sql.SQL(", ").join(safe_columns),
                        pk=pk_logic,
                        set_logic=final_set_logic
                    )
        # if there's no primary key, e.g. geolocation table
        else:
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


def create_logger():
    # 1. Configure the logger
    logger = logging.getLogger("pipeline_logger")
    logger.setLevel(logging.DEBUG)  # Capture everything from DEBUG up to CRITICAL

    # 2. Create a FileHandler to write errors to a specific file
    file_handler = logging.FileHandler("pipeline_errors.log", mode="a")
    file_handler.setLevel(logging.ERROR)  # ONLY log ERROR or CRITICAL messages to this file

    # 3. Create a Formatter to structure the log entries
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)

    # 4. Add the handler to the logger
    logger.addHandler(file_handler)
    
    return logger

PIPELINE_CONFIG = [
    {
        "csv_file": "olist_sellers_dataset.csv",
        "sql_file": "01_create_sellers_table.sql",
        "table_name": "stg_sellers",
        "primary_key": "seller_id"
    },
    {
        "csv_file": "olist_orders_dataset.csv",
        "sql_file": "02_create_orders_table.sql",
        "table_name": "stg_orders",
        "primary_key": "order_id"
    },
    {
        "csv_file": "olist_customers_dataset.csv",
        "sql_file": "03_create_customers_table.sql",
        "table_name": "stg_customers",
        "primary_key": "customer_id"
    },
    {
        "csv_file": "olist_order_items_dataset.csv",
        "sql_file": "04_create_order_items_table.sql",
        "table_name": "stg_order_items",
        "primary_key": ["order_id", "order_item_id"]
    },
    {
        "csv_file": "olist_order_payments_dataset.csv",
        "sql_file": "05_create_order_payments_table.sql",
        "table_name": "stg_order_payments",
        "primary_key": ["order_id", "payment_sequential"]
    },
    {
        "csv_file": "olist_order_reviews_dataset.csv",
        "sql_file": "06_create_order_reviews_table.sql",
        "table_name": "stg_order_reviews",
        "primary_key": "review_id"
    },
    {
        "csv_file": "olist_products_dataset.csv",
        "sql_file": "07_create_products_table.sql",
        "table_name": "stg_products",
        "primary_key": "product_id",
    },
    {
        "csv_file": "olist_geolocation_dataset.csv",
        "sql_file": "08_create_geolocation_table.sql",
        "table_name": "stg_geolocation",
        "primary_key": None
    },
    {
        "csv_file": "product_category_name_translation.csv",
        "sql_file": "09_create_category_name_translation_table.sql",
        "table_name": "stg_category_name_translation",
        "primary_key": "product_category_name"
    }
]

TRANSFORM_CONFIG = [
    "10_create_dim_products.sql",
    "11_create_dim_customers.sql",
    "12_create_dim_sellers.sql",
    "13_create_fact_sales.sql"
]

if __name__ == "__main__":
    logger = create_logger()
    conn = get_db_connection()
    for config in PIPELINE_CONFIG:
        try:
            # 1. Extract
            df = extract_data(config["csv_file"])

            # 2. Create Table (Staging)
            execute_sql_file(conn, config["sql_file"])

            # 3. Load Data
            bulk_insert_dataframe(conn, df, config["table_name"], config["primary_key"])
        except Exception as e:
            # exc_info=True automatically captures and appends the full stack trace
            logger.error(
                f"Failed execution for config: {config['csv_file']}.", 
                exc_info=True
            )
            continue
    
    execute_sql_file(conn, "09b_create_staging_indexes.sql")
    
    for config in TRANSFORM_CONFIG:
        try:
            execute_sql_file(conn, config)
        except Exception as e:
            logger.error(
                f"Failed execution for config: {config}.",
                exc_info=True
            )
            continue

    conn.close()