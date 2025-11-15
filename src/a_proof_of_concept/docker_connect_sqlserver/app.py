'''
a simple python app that queries sqlserver table
'''

import toml
import pymssql

# Load configuration from TOML file
config = toml.load('config.toml')
sql_config = config['sql_server_cdw']

# SQL Server connection parameters
host = sql_config['host']
database = sql_config['database']
user = sql_config['username']
password = sql_config['password']

SQL_QUERY = 'SELECT top(1) * FROM stage.idm.drupal__user_profiles'

def main_func(query):
    '''
    function for main logic
    '''

    # Connect to your database
    conn = pymssql.connect(
        host=host,
        user=user,
        password=password,
        database=database
        )

    # Create a cursor object
    cursor = conn.cursor()


    # Execute a query
    cursor.execute(query)

    # Fetch and print the results
    for row in cursor:
        print(row)

    # Close the connection
    conn.close()

if __name__ == "__app__":

    main_func(SQL_QUERY)
