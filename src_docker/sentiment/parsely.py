""" parsely api to gbq """
import os
import logging
import base64
import requests
import json
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from .logger import logger
from pandas_gbq import to_gbq
from common.aws.aws_secret import get_secret


load_dotenv()
logger("parselyapi")


def get_max_date_from_bigquery(table_id, client):
    """Query the latest date for which data is available in BigQuery."""
    try:
        query = f"SELECT MAX(pub_date) as max_date FROM `{table_id}`"
        query_job = client.query(query)
        result = query_job.to_dataframe()
        return result["max_date"].iloc[0]
    except Exception as e:
        logging.warning(
            f"Could not get max_date from {table_id}, assuming table does not exist. {e}"
        )
        return None


def append_to_bigquery(data, table_id, client, cred, project_id):
    try:
        # Check if the table exists
        client.get_table(table_id)
    except Exception as e:
        logging.info(f"Table {table_id} not found. Creating.")

        # Replace this with actual table schema creation logic
        to_gbq(
            data,
            destination_table=table_id,
            if_exists="replace",
            project_id=project_id,
            credentials=cred,
        )
        return

    # Append data to the existing table
    to_gbq(
        data,
        destination_table=table_id,
        if_exists="append",
        project_id=project_id,
        credentials=cred,
    )


def get_parsely_data(full_url, start_date, end_date, limit, key, secret):
    """Fetch Parsely data from API between given dates."""
    all_data = []
    for single_date in pd.date_range(start_date, end_date):
        pub_date = single_date.strftime("%Y-%m-%d")
        logging.info("Fetching data for %s", pub_date)
        page_number = 1
        while True:
            url = (
                f"{full_url}?apikey={key}&secret={secret}&pub_date_start={pub_date}"
                f"&pub_date_end={pub_date}&sort=pub_date&limit={limit}&page={page_number}"
            )
            try:
                response = requests.get(url, timeout=100)
                data = response.json()
                if not data.get("success", False):
                    break
                all_data.append(pd.DataFrame(data["data"]))
                page_number += 1
            except Exception as error:
                logging.error("An error occurred: %s", error)
                break
    return (
        pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    )


def parsely_run():

    # get secrets from aws
    sentiment_value = get_secret("datateam_sentiment")
    sentiment_value_json = json.loads(sentiment_value)
    PROJECT_ID = sentiment_value_json["PROJECT_ID"]
    DATASET_ID = sentiment_value_json["DATASET_ID"]
    TABLE_NAME = sentiment_value_json["TABLE_NAME"]
    TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
    START_DATE = "2023-01-01"
    ENDPOINT = sentiment_value_json["ENDPOINT"]
    FULL_URL = f"http://api.parsely.com/v2/{ENDPOINT}"

    encoded_cred = get_secret("datateam_google_cred_prod_base64")
    encoded_value = json.loads(encoded_cred)
    encoded_cred = encoded_value["GOOGLE_CLOUD_CRED_BASE64"]
    decoded_cred = base64.b64decode(encoded_cred).decode("utf-8")
    google_cred = json.loads(decoded_cred)

    parsely_key = sentiment_value_json["PARSELY_API_KEY"]
    parsely_secret = sentiment_value_json["PARSELY_API_SECRET"]
    LIMIT = 1000

    credentials = Credentials.from_service_account_info(google_cred)

    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    max_date = get_max_date_from_bigquery(TABLE_ID, client)
    start_date_to_query = (
        pd.Timestamp(max_date) + pd.Timedelta(days=1)
        if max_date
        else pd.Timestamp(START_DATE)
    )
    end_date_to_query = pd.Timestamp(
        pd.to_datetime("today").strftime("%Y-%m-%d")
    )

    if start_date_to_query <= end_date_to_query:
        parsely_data = get_parsely_data(
            FULL_URL,
            start_date_to_query,
            end_date_to_query,
            LIMIT,
            parsely_key,
            parsely_secret,
        )

        # Convert all list columns to JSON strings
        for col in parsely_data.columns:
            if parsely_data[col].apply(lambda x: isinstance(x, list)).any():
                parsely_data[col] = parsely_data[col].apply(json.dumps)

        # Serialize all dict columns to JSON strings
        for col in parsely_data.columns:
            if parsely_data[col].apply(lambda x: isinstance(x, dict)).any():
                parsely_data[col] = parsely_data[col].apply(json.dumps)

        if not parsely_data.empty:
            append_to_bigquery(
                parsely_data, TABLE_ID, client, credentials, PROJECT_ID
            )
            logging.info("Appended %s records.", len(parsely_data))
    else:
        logging.info("No new data to fetch.")
