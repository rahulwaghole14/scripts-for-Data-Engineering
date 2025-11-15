"""
this
"""
# pylint: disable=all

import logging
import os

import pandas as pd
import s3fs  # pylint: disable=import-error
import sqlalchemy
from create_uuid import generate_uuid
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# Parameters
TABLE_NAME = "print_data_generic_histo"
server = os.getenv("sql_server")
database = os.getenv("sql_database")
username = os.getenv("sql_username")
password = os.getenv("sql_password")
sql_driver = os.getenv("sql_driver")
uuid_namespace = os.getenv("UUID_NAMESPACE")

# set up logging


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# get vars
bucket_name = os.getenv("bucket_name")
file_name = os.getenv("file_name_generic_histo")
s3_key = os.getenv("s3_key")
s3_secret = os.getenv("s3_secret")
aws_region = os.getenv("aws_region")
logger.info(uuid_namespace)

fs = s3fs.S3FileSystem(key=s3_key, secret=s3_secret)

# Use 'w' for py3, 'wb' for py2
with fs.open(f"{bucket_name}/{file_name}", "rb") as f:
    dataframe = pd.read_csv(f, encoding="latin1")


logging.info(dataframe.columns)

# # Existing DataFrame column names
# INFO:root:Index(['Action (History of service subscription)',
#        'Date (History of service subscription)',
#        'Internal name (History of service subscription/Service)', 'Email'],
old_columns = [
    "Action (History of service subscription)",
    "Date (History of service subscription)",
    "Internal name (History of service subscription/Service)",
    "Email",
]

# # # New column names
new_columns = ["action", "date", "internal_name", "email"]

# Create a mapping from old column names to new column names
column_mapping = dict(zip(old_columns, new_columns))

# Rename the DataFrame columns
dataframe = dataframe.rename(columns=column_mapping)

# Now, dataframe should have the new column names
logger.info(dataframe.head())

# count row of dataframe
logger.info(dataframe.shape)

# count row of dataframe
# drop data email is invalid:
logger.info("removing invalid email")
dataframe = dataframe[dataframe["email"].str.contains("@", na=False)]

logger.info(dataframe.shape)

# email field in dataframe trim and lowercase:
dataframe["email"] = dataframe["email"].str.lower().str.strip()
dataframe["marketing_id"] = dataframe["email"].apply(
    lambda x: generate_uuid(x, uuid_namespace)
)  # generate marketing id from email


# Convert and format 'canx_date' column (YYYY/MM/DD hh:mm:ss)
dataframe["date"] = pd.to_datetime(dataframe["date"], errors="coerce")
# drop all columns except for

logger.info(dataframe.head())

# print datatypes in the dataframe:

logging.info(dataframe.dtypes)

# Create the connection string
conn = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    f"?driver={sql_driver}&TrustServerCertificate=yes"
)
engine = sqlalchemy.create_engine(conn, fast_executemany=True)

# Write DataFrame to SQL Server
dataframe.to_sql(
    TABLE_NAME,
    engine,
    schema="acm",
    if_exists="replace",
    dtype={
        "action": sqlalchemy.types.String(),
        "date": sqlalchemy.types.DateTime(),
        "internal_name": sqlalchemy.types.String(),
        "email": sqlalchemy.types.String(),
        "marketing_id": sqlalchemy.types.String(),
    },
    chunksize=50000,
)
