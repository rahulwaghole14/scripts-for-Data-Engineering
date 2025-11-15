import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

# Define your connection details
server = os.getenv('MATRIX_DB_SERVER')
database = os.getenv('MATRIX_DB_DATABASE')
username = os.getenv('MATRIX_DB_USERNAME')
password = os.getenv('MATRIX_DB_PASSWORD')
portnumber = os.getenv('MATRIX_DB_PORT')

# Define the connection string
conn_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{portnumber};DATABASE={database};UID={username};PWD={password}'

# Print the connection string for debugging (remove this in production for security reasons)
print("Connection String:", conn_string)

try:
    # Establish connection
    with pyodbc.connect(conn_string, timeout=10) as conn:  # Added a timeout for the connection attempt
        print("Connection successful.")

        # Perform database operations here
        with conn.cursor() as cursor:
            cursor.execute("SELECT TOP (1) * FROM tbl_person")  # Simplified for demonstration
            for row in cursor:
                print(row)

except pyodbc.Error as e:
    print("Error in connection:", e)