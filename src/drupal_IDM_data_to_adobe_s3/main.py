
"""
This script processes user data from a BigQuery table, transforms it, and uploads the processed data to an AWS S3 bucket. 
It is designed to run both daily updates and weekly overwrites based on the day of the week.

Main Functions:
    process_record(record): Processes individual records from the DataFrame.
    get_data_for_daily_update(): Fetches and processes data for daily updates.
    get_data_for_weekly_overwrite(): Fetches and processes data for weekly overwrites.
    upload_to_s3(file_content, bucket, file_name): Uploads processed data to an S3 bucket.
    main(): The main function that orchestrates data processing and uploading.
"""
import time
import inspect 
import os
import logging
import json
import re
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import s3fs
from datetime import datetime, date
from dotenv import load_dotenv
import boto3
from botocore.exceptions import NoCredentialsError
from load_config import load_config


# Setup logging
def setup_logger():
    log_directory = "logs"
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler(os.path.join(log_directory, "data_upload.log")),
                            logging.StreamHandler()
                        ])
    logger = logging.getLogger()
    return logger

logger = setup_logger()

load_dotenv()

# Load S3 credentials from .env
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

def format_country_name(country_name):
    """
    Formats the country name by adding a space before each capital letter 
    in concatenated country names and making the entire string lowercase.
    Args:
        country_name (str): The country name to format.
    Returns:
        str: Formatted country name.
    """
    if not country_name:
        return ''
    
    # Adding a space before each capital letter and converting to lowercase
    formatted_name = re.sub(r"(\w)([A-Z])", r"\1 \2", country_name).lower()
    return formatted_name



def format_postcode(postcode, country):
    """Format the postcode with padding if the country is New Zealand."""
    if country == 'New Zealand' and postcode and len(str(postcode)) <= 4:
        return str(postcode).zfill(4)
    return None

def process_record(record):
    # Gender mapping
    gender_map = {
        'Female': 'f', 'Woman': 'f', 'F': 'f', 'Male': 'm', 'Man': 'm', 'M': 'm',
        'Non-Binary': 'nb', 'Takatapui': 't', 'Rather not say': 'u', 'Rather-Not-Say': 'u',
        'U': 'u'
    }
    gender = gender_map.get(record.get('gender'), None)

    # Format country name and postcode
    country = f"\"{format_country_name(record.country)}\"" if record.country else ''
    
    # Extract year of birth
    yob = record.get('year_of_birth')
    yob = yob.split('-')[0] if yob and yob != 'NaT' else ''
    
    # Postcode processing
    postcode = format_postcode(record.postcode, record.country)

    # Newsletter subscription processing
    newsletter_subs_entries = []
    if record.newsletter_subs:
        try:
            subs = json.loads(record.newsletter_subs.replace("'", "\""))
            for sub in subs:
                if 'acmNewsInternalName' in sub:
                    newsletter_subs_entries.append(f"newsletter_sub=\"{sub['acmNewsInternalName']}\"")
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON: {e}")

    # Combine all non-empty fields into a list
    processed_values = [f"g={gender}" if gender else "", 
                        f"country={country}" if country else "", 
                        f"yob={yob}" if yob else "",
                        f"pc={postcode}" if postcode else ""] + newsletter_subs_entries
    # Filter out empty values and join using commas
    processed_record = ','.join(filter(None, processed_values))

    # Combine user_id with the processed_record using tab delimiter
    return f"{record.user_id}\t{processed_record}"



# Function to get and process data from BigQuery
def get_data_for_daily_update():
    config = load_config()
    
    # Setup Google Cloud credentials and BigQuery client
    credentials = service_account.Credentials.from_service_account_info(config["GOOGLE_CLOUD_CRED"])
    client = bigquery.Client(credentials=credentials, project=config["PROJECT_ID"])

    # Query for the most recent data
    query = f"""
        SELECT user_id, gender, newsletter_subs, country, year_of_birth, record_load_dts_utc, postcode
        FROM `{config["PROJECT_ID"]}.{config["DATASET_ID"]}.{config["TABLE_ID"]}`
        WHERE record_load_dts_utc = (
            SELECT MAX(record_load_dts_utc) 
            FROM `{config["PROJECT_ID"]}.{config["DATASET_ID"]}.{config["TABLE_ID"]}`
                    )
            AND gender is not null
            AND country is not null
            AND year_of_birth is not null
            AND postcode is not null
            AND newsletter_subs is not null
    """
    df = client.query(query).to_dataframe()
    row_count = len(df)
    max_record_load_dts_utc = df['record_load_dts_utc'].max()
    logger.info(f"{inspect.currentframe().f_code.co_name} - Number of rows processed: {row_count}")
    logger.info(f"{inspect.currentframe().f_code.co_name} - Max record_load_dts_utc: {max_record_load_dts_utc}")
    print("DataFrame after fetching from BigQuery (Daily Update):")
    print(df.head())  # Print first few rows of the DataFrame
    df['processed_record'] = df.apply(process_record, axis=1)
    print(df[['processed_record']].head())
    return df

def get_data_for_weekly_overwrite():
    config = load_config()
    
    credentials = service_account.Credentials.from_service_account_info(config["GOOGLE_CLOUD_CRED"])
    client = bigquery.Client(credentials=credentials, project=config["PROJECT_ID"])

    query = f"""
        SELECT user_id, gender, newsletter_subs, country, year_of_birth, record_load_dts_utc, postcode
        FROM `{config["PROJECT_ID"]}.{config["DATASET_ID"]}.{config["TABLE_ID"]}`
        WHERE TIMESTAMP(record_load_dts_utc) >= TIMESTAMP('{config["INITIAL_DATE"]}')
        AND record_load_dts_utc <= (
            SELECT MAX(record_load_dts_utc) 
            FROM `{config["PROJECT_ID"]}.{config["DATASET_ID"]}.{config["TABLE_ID"]}`
        )
        AND gender is not null
        AND country is not null
        AND year_of_birth is not null
        AND postcode is not null
        AND newsletter_subs is not null
    """
    df = client.query(query).to_dataframe()
    row_count = len(df)
    max_record_load_dts_utc = df['record_load_dts_utc'].max()
    logger.info(f"{inspect.currentframe().f_code.co_name} - Number of rows processed: {row_count}")
    logger.info(f"{inspect.currentframe().f_code.co_name} - Max record_load_dts_utc: {max_record_load_dts_utc}")
    print("DataFrame after fetching from BigQuery (Weekly Overwrite):")
    print(df.head())
    df['processed_record'] = df.apply(process_record, axis=1)
    print(df[['processed_record']].head()) 
    return df

def upload_to_s3(file_content, full_path):
    """
    Uploads the processed data string to an AWS S3 bucket.
    Args:
        file_content (str): The content to upload.
        full_path (str): The full path (bucket and key) where the file will be uploaded in S3.
    """
    # Local test write
    local_test_file = "local_test_output.csv"
    with open(local_test_file, 'w', newline='', encoding='utf-8') as f:
        f.write(file_content)
    logger.info(f"Local test data saved to {local_test_file}")

    # Actual upload to S3
    logger.info(f"Uploading to S3: {full_path}")

    fs = s3fs.S3FileSystem(key=ACCESS_KEY, secret=SECRET_KEY)
    with fs.open(full_path, 'wb') as f:
        f.write(file_content.encode('utf-8'))  # Encode to bytes
    logger.info(f"Data uploaded to S3: {full_path}")
   
def main():
    logger.info("Starting data processing and upload")

    today = date.today()
    day_of_week = today.weekday()  # Monday is 0, Sunday is 6
    t = int(round(time.time()))  # Current time in seconds

    if day_of_week == 6:  # Assuming Sunday for weekly overwrite
        processed_data = get_data_for_weekly_overwrite()
        file_name = f'ftp_dpm_1272979_{t}.overwrite'
    else:
        processed_data = get_data_for_daily_update()
        file_name = f'ftp_dpm_1272979_{t}.sync'

    bucket = os.getenv('S3_BUCKET')
    s3_dir = f"{os.getenv('S3_KEY_PREFIX')}/date={today.isoformat()}/"
    full_path = f"{bucket}/{s3_dir}{file_name}"

    # Upload to S3
    upload_to_s3(processed_data['processed_record'].str.cat(sep='\n'), full_path)

    logger.info(f"Data upload complete: {full_path}")

if __name__ == '__main__':
    main()