import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from extract_data import extract_data_for_engagement_table
from adobe.get_token import get_token
from bigquery.append_data import append_dataframe_to_bigquery
from bigquery.create_client import create_bigquery_client
from bigquery.create_schema import create_bigquery_schema
from bigquery.schema_mapping import schema_map_dict

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
CLIENT_ID = os.environ.get("ADOBE_CLIENT_ID")
CLIENT_SECRET = os.environ.get("ADOBE_CLIENT_SECRET")
GOOGLE_CLOUD_CRED = os.environ.get("GOOGLE_CLOUD_CRED")
GOOGLE_DATASET = "hexa_new_registration"
GOOGLE_TABLE_ID = "src_adobe_analytics__registrations"
COMPANY_ID = 'fairfa5'

def get_yesterday_date():
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def process_yesterday_data():
    try:
        # Get Adobe token
        token = get_token(CLIENT_ID, CLIENT_SECRET)
        access_token = token['access_token']
        
        # Set API request headers
        request_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "x-api-key": CLIENT_ID,
            "x-proxy-global-company-id": COMPANY_ID
        }

        # Get yesterday's date
        yesterday = get_yesterday_date()
        logging.info(f"Processing data for date: {yesterday}")

        # Extract data
        data = extract_data_for_engagement_table(yesterday, request_headers)

        if not data.empty:
            # Create BigQuery client and schema
            client = create_bigquery_client(GOOGLE_CLOUD_CRED)
            schema = create_bigquery_schema(schema_map_dict)

            # Prepare data for BigQuery
            data["date_at"] = pd.to_datetime(yesterday)
            data["date_load_at"] = datetime.now()

            # Append data to BigQuery
            table_id = f"{GOOGLE_DATASET}.{GOOGLE_TABLE_ID}"
            append_dataframe_to_bigquery(data, client, table_id, schema)
            logging.info(f"Data for {yesterday} appended to BigQuery")
        else:
            logging.info(f"No data found for date: {yesterday}")

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    process_yesterday_data()