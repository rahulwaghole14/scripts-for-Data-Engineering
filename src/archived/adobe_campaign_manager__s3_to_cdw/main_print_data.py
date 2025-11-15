"""
this
"""
# pylint: disable=all

import logging
import os
import sys

import boto3
from dotenv import load_dotenv
import pandas as pd
import sqlalchemy
from create_uuid import generate_uuid

# load environment variables from .env file
load_dotenv()

# Parameters
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
file_name = os.getenv("file_name")
s3_key = os.getenv("s3_key")
s3_secret = os.getenv("s3_secret")
aws_region = os.getenv("aws_region")
# logger.info("Bucket name: %s, File name: %s, S3 key: %s, S3 secret: %s", bucket_name, file_name, s3_key, s3_secret)
logger.info(uuid_namespace)

try:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_key,
        aws_secret_access_key=s3_secret,
        region_name=aws_region,
    )
except Exception as error:
    logging.info("s3 client error %s", error)


def get_file_from_s3(bucket_name, file_name):
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    return obj


try:
    data = get_file_from_s3(bucket_name, file_name)
except Exception as error:
    logging.info("s3 get_file_from_s3 error %s", error)

try:
    dataframe = pd.read_csv(data["Body"])
except Exception as error:
    logging.info("dataframe error %s", error)
    sys.exit(1)  # Exit the script if there was an error.

# Existing DataFrame column names
old_columns = [
    "Email",
    "Internal name (Subscriptions/Service)",
    "Product ID (Matrix Subscription)",
    "End Date (Matrix Subscription)",
    "Cancellation Date (Matrix Subscription/Cancellations)",
    "Expected End Date (Matrix Subscription)",
]

# New column names
new_columns = [
    "email",
    "internal_name",
    "product_id",
    "end_date",
    "canx_date",
    "expected_end_date",
]

# Create a mapping from old column names to new column names
column_mapping = dict(zip(old_columns, new_columns))

# Rename the DataFrame columns
dataframe = dataframe.rename(columns=column_mapping)

# Now, dataframe should have the new column names
logger.info(dataframe.head())

# use sqlalchemy get this data into sqlserver 2017 new table in stage database acm schema called print_data (table does not exist yet)

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
)  # generate marketing id from email for match to matrix marketing_id, if not match match on email...


# Convert and format 'canx_date' column (YYYY/MM/DD hh:mm:ss)
dataframe["canx_date"] = pd.to_datetime(
    dataframe["canx_date"], errors="coerce"
)
dataframe["end_date"] = pd.to_datetime(dataframe["end_date"], errors="coerce")
dataframe["expected_end_date"] = pd.to_datetime(
    dataframe["expected_end_date"], errors="coerce"
)

logger.info(dataframe.head())


# print datatypes in the dataframe:

logging.info(dataframe.dtypes)

# Create the connection string
conn = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={sql_driver}&TrustServerCertificate=yes"
engine = sqlalchemy.create_engine(conn, fast_executemany=True)

# Write DataFrame to SQL Server
dataframe.to_sql(
    "print_data",
    engine,
    schema="acm",
    if_exists="replace",
    dtype={
        "email": sqlalchemy.types.String(),
        "internal_name": sqlalchemy.types.String(),
        "product_id": sqlalchemy.types.String(),
        "end_date": sqlalchemy.types.DateTime(),
        "canx_date": sqlalchemy.types.DateTime(),
        "expected_end_date": sqlalchemy.types.DateTime(),
        "marketing_id": sqlalchemy.types.String(),
    },
    chunksize=50000,
)
