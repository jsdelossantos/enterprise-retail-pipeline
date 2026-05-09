import os
import psycopg2
from dotenv import load_dotenv

def test_connection():
    # 1. The Ignition: Load the hidden variables
    load_dotenv()

    # Initialize connection variable
    connection = None

    try:
        # 2. The Handshake: Connect using environment variables
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS")
        )
        
        # 3. The Test Workspace
        cursor = connection.cursor()
        
        # 4. The Ping
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        print(f"Success! Connected to: {db_version[0]}")
        
        cursor.close()

    except Exception as error:
        print(f"Failed to connect to the database. Error: {error}")

    finally:
        # 5. The Cleanup
        if connection is not None:
            connection.close()
            print("Database connection successfully closed.")

if __name__ == "__main__":
    test_connection()