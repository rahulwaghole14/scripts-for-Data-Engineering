''' main '''
# pylint: disable=import-error

import datetime
import logging
import os

# import pandas as pd
import sqlalchemy
from sqlalchemy.util import deprecations
from api_idm.batch_deduplicate_backfill import (batch_get_users,
                                                write_and_merge_data,
                                                save_progress)
from api_idm.clean_data import (map_columns,
                                read_list_of_dicts, remove_duplicates,
                                replace_non_utf8)
from dotenv import load_dotenv
from logger import logger
# from api_idm.create_uuid import generate_uuid
# import pandas as pd

logger('idm-backfill')

# Load the environment variables from .env file
deprecations.SILENCE_UBER_WARNING = True
load_dotenv()

# filter params
now = datetime.datetime.utcnow()
delta = datetime.timedelta(hours=48*7)
before = now - delta
timestamp = before.strftime("%Y-%m-%dT%H:%M:%SZ")

# set mode 0 = get data between dates,
# 1 = get all data from date,
# 2 = get data between dates and filter on id,
MODE = 1

# START_DATE = timestamp # 24 hours before now utc
END_DATE = None
# START_DATE = timestamp
START_DATE = "2023-07-28T00:00:00Z"
COUNT = 10
WAIT_TIME = 10 # seconds

TABLE_NAME = "drupal__user_profiles"
SCHEMA_NAME = "idm"

# uuid namespace
UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")

# API endpoint and credentials
# API_URL = "https://account-management-stage.staging.nebula-drupal.hexa.co.nz"
API_URL = "https://accounts.hexa.co.nz"
username = os.environ.get("API_USERNAME")
password = os.environ.get("API_PASSWORD")
apiauth = os.environ.get("API_AUTH")

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")

# Set up the database connection
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={DRIVER}&TrustServerCertificate=yes"
engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

start_datetime = '2023-08-04T00:00:00Z'
end_datetime = '2023-08-08T00:00:00Z'

if __name__ == '__main__':

    # Call the function in a loop
    for output in batch_get_users(API_URL, username, password, apiauth, start_datetime, end_datetime):
        data = output['data']
        # Convert the batch of results into a DataFrame
        data = read_list_of_dicts(data)
        data = replace_non_utf8(data)
        data = map_columns(data, UUID_NAMESPACE)
        data = remove_duplicates(data)

        # Write the new data to the SQL table
        write_and_merge_data(data, engine)

        # Save progress after writing data to SQL server
        save_progress(output['date'], output['start_index'], output['count'], output['num_iterations'])
