""" main app """
# pylint: disable=import-error

import json
import logging
import os
import requests
from datetime import date, datetime, timedelta

import pandas as pd
from adobe.get_token import get_token
from dotenv import load_dotenv
from extract_data import extract_data_for_engagement_table

# from google.cloud import bigquery
from logger import logger

from bigquery.append_data import append_dataframe_to_bigquery
from bigquery.create_client import create_bigquery_client
from bigquery.create_schema import create_bigquery_schema

# from bigquery.create_table import create_bigquery_table
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
# set api request params
    REQUEST_HEADERS = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "x-api-key": CLIENT_ID,  # Add this line
        "x-proxy-global-company-id": COMPANY_ID  # Add this line
    }
    logging.info("Request Headers: %s", REQUEST_HEADERS)
except Exception as error:
    logging.error("Error getting token or setting headers: %s", error)



base_url = 'https://analytics.adobe.io/api/fairfa5'
headers = {
    'Authorization': f'Bearer {access_token}',
    'x-api-key': '1ee53bf3f35047358777c889d249b870',
    'x-proxy-global-company-id': 'fairfa5'
}

# Replace with your report suite ID
report_suite_id = "fairfaxnz-hexaoverall-production"

response = requests.get(f"{base_url}/reportsuites/dateranges/{report_suite_id}", headers=headers)

if response.status_code == 200:
    date_ranges = response.json()
    print(json.dumps(date_ranges, indent=2))
else:
    print(f"Error: {response.status_code}")
    print(response.text)