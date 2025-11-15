"""
This script reads a CSV file from an S3 bucket
and writes it to a SQL Server table.
"""
# pylint: disable=all

import logging
import os
import sys
from io import StringIO  # This line is new

import boto3  # pylint: disable=import-error
import chardet  # pylint: disable=import-error
import pandas as pd
import sqlalchemy
from create_uuid import generate_uuid
from dotenv import load_dotenv

# load environment variables from .env file
load_dotenv()

# Parameters
TABLE_NAME = "print_data_blacklist"
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
file_name = os.getenv("file_name_blacklist")
s3_key = os.getenv("s3_key")
s3_secret = os.getenv("s3_secret")
aws_region = os.getenv("aws_region")
logger.info(uuid_namespace)

try:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=s3_key,
        aws_secret_access_key=s3_secret,
        region_name=aws_region,
    )
except Exception as error:  # pylint: disable=broad-except
    logging.info("s3 client error %s", error)


def get_file_from_s3(bucket, file):
    """Get file from s3 bucket."""
    obj = s3.get_object(Bucket=bucket, Key=file)
    return obj


try:
    data = get_file_from_s3(bucket_name, file_name)
except Exception as error:  # pylint: disable=broad-except
    logging.info("s3 get_file_from_s3 error %s", error)

try:
    # Read the content of the file into bytes
    byte_content = data["Body"].read()

    # Determine the encoding using chardet
    result = chardet.detect(byte_content)

    # Convert bytes to string using the detected encoding
    string_content = byte_content.decode(result["encoding"])

    # Use StringIO to handle the string content like a file object
    file_like_object = StringIO(string_content)

    # Load the data into a DataFrame
    dataframe = pd.read_csv(file_like_object)

    # dataframe = pd.read_csv(data['Body'], encoding='latin1')
except Exception as error:  # pylint: disable=broad-except
    logging.info("dataframe error %s", error)
    sys.exit(1)  # Exit the script if there was an error.


logging.info(dataframe.columns)

# # Existing DataFrame column names


old_columns = ["Address", "Type", "Status", "Error reason", "Update status"]

# # New column names
new_columns = ["address", "type", "status", "error_reason", "update_status"]

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
dataframe = dataframe[dataframe["address"].str.contains("@", na=False)]

logger.info(dataframe.shape)

# email field in dataframe trim and lowercase:
dataframe["address"] = dataframe["address"].str.lower().str.strip()
# generate marketing id from email
dataframe["marketing_id"] = dataframe["address"].apply(
    lambda x: generate_uuid(x, uuid_namespace)
)


# Convert and format 'canx_date' column (YYYY/MM/DD hh:mm:ss)
# dataframe['date'] = pd.to_datetime(dataframe['date'], errors='coerce')
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
        "address": sqlalchemy.types.String(),
        "type": sqlalchemy.types.String(),
        "status": sqlalchemy.types.String(),
        "error_reason": sqlalchemy.types.String(),
        "update_status": sqlalchemy.types.String(),
        "marketing_id": sqlalchemy.types.String(),
    },
    chunksize=50000,
)
