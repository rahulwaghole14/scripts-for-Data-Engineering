""" main app """
# pylint: disable=import-error

import json
import logging
import os
from datetime import date, datetime, timedelta

import pandas as pd
from adobe.get_token import get_token
from dotenv import load_dotenv
from extract_data import extract_data_for_engagement_table

from logger import logger

from bigquery.append_data import append_dataframe_to_bigquery
from bigquery.create_client import create_bigquery_client
from bigquery.create_schema import create_bigquery_schema
from bigquery.get_dates import get_dates
from bigquery.map_dataframe import map_dataframe
from bigquery.schema_mapping import schema_map_dict

logger("adobe_rego")

try:
    load_dotenv()
    CLIENT_ID = os.environ.get("ADOBE_CLIENT_ID")
    CLIENT_SECRET = os.environ.get("ADOBE_CLIENT_SECRET")
    GOOGLE_CLOUD_CRED = os.environ.get("GOOGLE_CLOUD_CRED")
    GOOGLE_DATASET = "hexa_new_registration"
    GOOGLE_TABLE_ID = "src_adobe_analytics__registrations"
    COMPANY_ID = 'fairfa5'
    base_url = 'https://analytics.adobe.io/api/' + COMPANY_ID
    logging.info("got environment variables")
except Exception as error:  # pylint: disable=broad-except
    logging.info(error)

try:
    # get token
    token = get_token(CLIENT_ID, CLIENT_SECRET)
    logging.info("got adobe token: %s", token)
    access_token = token['access_token']
    logging.info("got adobe token: %s", access_token)
    
    # set api request params
    REQUEST_HEADERS = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "x-api-key": CLIENT_ID,
        "x-proxy-global-company-id": COMPANY_ID
    }
    logging.info("Request Headers: %s", REQUEST_HEADERS)
except Exception as error:
    logging.error("Error getting token or setting headers: %s", error)

schema = create_bigquery_schema(schema_map_dict)

def validate_date_range(start_date, end_date):
    today = date.today()
    
    if start_date > today:
        raise ValueError("Start date cannot be in the future")
    
    if end_date > today:
        logging.warning("End date is in the future. Setting to today's date.")
        end_date = today
    
    if start_date > end_date:
        raise ValueError("Start date cannot be after end date")
    
    max_range = timedelta(days=365)  # For example, limit to 1 year
    if end_date - start_date > max_range:
        raise ValueError(f"Date range cannot exceed {max_range.days} days")
    
    return start_date, end_date

def process_date_range(start_date, end_date):
    try:
        client = create_bigquery_client(GOOGLE_CLOUD_CRED)
        schema = create_bigquery_schema(schema_map_dict)
        
        current_date = start_date
        dates_appended = 0
        
        while current_date <= end_date:
            try:
                logging.info("Processing date: %s", current_date)
                date_for_adobe = current_date.strftime("%Y-%m-%d")
                data = extract_data_for_engagement_table(
                    date_for_adobe, REQUEST_HEADERS
                )
                if not data.empty:
                    data["date_at"] = pd.to_datetime(current_date)
                    data["date_load_at"] = datetime.now()
                    df = map_dataframe(data, schema_map_dict)
                    
                    # Load data into BigQuery immediately after processing
                    append_dataframe_to_bigquery(
                        df, client, GOOGLE_DATASET + "." + GOOGLE_TABLE_ID, schema
                    )
                    dates_appended += 1
                    logging.info("Data for %s appended to BigQuery", current_date)
                else:
                    logging.info("No data found for date: %s", current_date)
            except Exception as e:
                logging.error("Error processing date %s: %s", current_date, str(e))
            finally:
                current_date += timedelta(days=1)
        
        logging.info("Total dates appended to BigQuery: %s", dates_appended)
    except Exception as e:
        logging.error("Error in process_date_range: %s", str(e))

if __name__ == "__main__":
    start_date_str = input("Enter start date (YYYY-MM-DD): ")
    end_date_str = input("Enter end date (YYYY-MM-DD): ")
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        
        start_date, end_date = validate_date_range(start_date, end_date)
        process_date_range(start_date, end_date)
    except ValueError as e:
        logging.error("Date validation error: %s", str(e))
    except Exception as e:
        logging.error("An unexpected error occurred: %s", str(e))