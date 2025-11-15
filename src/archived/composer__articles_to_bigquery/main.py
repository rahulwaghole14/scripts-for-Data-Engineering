''' main app '''
# pylint: disable=all
import logging
import os
import base64
import time
import json
import warnings
import pandas as pd
import sqlalchemy
from api_composer.clean_articles import add_columns
from api_composer.get_articles import (
    get_articles_list,
    get_articles_to_dataframe,
    init_session
    )
from pandas_gbq import to_gbq
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Suppress specific warning
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

# Load the environment variables from .env file
load_dotenv()
PAGELIMIT = 500

# normal behavior:
day_before_yesterday = (pd.Timestamp.today() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
first_day = day_before_yesterday

# batch update history:
# between two dates:
# first_day = '2023-05-01'
# last_day = '2023-05-31'

TABLE_NAME = "composer__articles"
SCHEMA_NAME = "composer"

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")

# api endpoint details
COMPOSER_API_URL = os.environ.get("COMPOSER_API_URL")
url = (
    f"{COMPOSER_API_URL}"
    f"?direction=asc&orderby=assetId&limit={PAGELIMIT}"
    f"&q=(firstPublishTime%3Dge%3D{first_day}T00%3A00%3A00%3B"
    # f"firstPublishTime%3Dle%3D{last_day}T23%3A59%3A59.999%3B" # between two dates
    f"status%3Dmatch%3DREADY%3BlastActionPerformed%3Dmatch%3DPUBLISH)"
    )

# Set up the database connection
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={DRIVER}&TrustServerCertificate=yes"
engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
conn_str_smart = f"mssql+pyodbc://{username}:{password}@{server}/smart?driver={DRIVER}&TrustServerCertificate=yes"
engine_smart = sqlalchemy.create_engine(conn_str_smart, fast_executemany=True)

if __name__ == '__main__':

    PROJECT_ID = "hexa-data-report-etl-prod"
    DATASET_ID = "cdw_stage"
    TEMP_TABLE = "temp_composer__articles"
    TEMP_ID = DATASET_ID + "." + TEMP_TABLE

    encoded_cred = os.getenv("GOOGLE_CLOUD_CRED_BASE64")
    decoded_cred = base64.b64decode(encoded_cred).decode('utf-8')

    google_cred = json.loads(decoded_cred)
    credentials = Credentials.from_service_account_info(google_cred)

    ## start timing
    start = time.time()
    session = init_session()

    ## get a list of article ids
    # data = [85811271,300892591,128296235,77536927,300942622,300970485]
    data = get_articles_list(url, session)

    # print("articles to get between dates: " + first_day + " and " + last_day + " are: " + str(len(data))) # between two dates
    logging.info("articles to get between dates: %s and NOW are: %s", first_day, str(len(data)))

    ## create dataframe
    data_frame = get_articles_to_dataframe(data, COMPOSER_API_URL, session)
    session.close()
    data_frame = add_columns(data_frame)

    # Upload DataFrame to temp table
    credentials = Credentials.from_service_account_info(google_cred)
    to_gbq(data_frame, TEMP_ID, project_id=PROJECT_ID, if_exists='replace')
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    MERGE_SQL = """
    MERGE INTO cdw_stage.composer__articles AS target
    USING cdw_stage.temp_composer__articles AS source
    ON target.content_id = source.content_id

    -- Update existing rows
    WHEN MATCHED THEN
    UPDATE SET
        title = source.title,
        published_dts = source.published_dts,
        source = source.source,
        brand = source.brand,
        category = source.category,
        category_1 = source.category_1,
        category_2 = source.category_2,
        category_3 = source.category_3,
        category_4 = source.category_4,
        category_5 = source.category_5,
        category_6 = source.category_6,
        print_slug = source.print_slug,
        author = source.author,
        word_count = source.word_count,
        image_count = source.image_count,
        video_count = source.video_count,
        advertisement = source.advertisement,
        sponsored = source.sponsored,
        promoted_flag = source.promoted_flag,
        comments_flag = source.comments_flag,
        home_flag = source.home_flag,
        hash_key = source.hash_key,
        hash_diff = source.hash_diff,
        record_source = source.record_source,
        sequence_nr =  CAST(source.sequence_nr AS FLOAT64),
        record_load_dts_utc = source.record_load_dts_utc,
        load_dt = source.load_dt

    -- Insert new rows
    WHEN NOT MATCHED THEN
    INSERT (
        content_id, title, published_dts, source, brand, category,
        category_1, category_2, category_3, category_4, category_5, category_6,
        print_slug, author, word_count, image_count, video_count,
        advertisement, sponsored, promoted_flag, comments_flag, home_flag,
        hash_key, hash_diff, record_source, sequence_nr,
        record_load_dts_utc, load_dt
    )
    VALUES (
        source.content_id, source.title, source.published_dts, source.source, source.brand, source.category,
        source.category_1, source.category_2, source.category_3, source.category_4, source.category_5, source.category_6,
        source.print_slug, source.author, source.word_count, source.image_count, source.video_count,
        source.advertisement, source.sponsored, source.promoted_flag, source.comments_flag, source.home_flag,
        source.hash_key, source.hash_diff, source.record_source, CAST(source.sequence_nr AS FLOAT64),
        source.record_load_dts_utc, source.load_dt
    );
    """

    query_job = client.query(MERGE_SQL)
    query_job.result()

    # Remove the temporary table
    client.delete_table(TEMP_ID, not_found_ok=True)

    ## stop timer
    end = time.time()
    logging.info("total time taken: %s", str(end - start))
