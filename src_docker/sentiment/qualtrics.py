import json
import os
import warnings
import zipfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq
from common.aws.aws_secret import get_secret
from .logger import logger
import logging


def initialize_config():
    """setup config"""
    load_dotenv()

    try:
        sentiment_value = get_secret("datateam_sentiment")
        sentiment_json = json.loads(sentiment_value)
        api_token = sentiment_json["QUALTRICS_API_TOKEN"]
        survey_id = sentiment_json["QUALTRICS_SURVEY_ID"]
        server = sentiment_json["QUALTRICS_DATA_CENTER"]
        project_id = sentiment_json["PROJECT_ID"]
        dataset_id = sentiment_json["DATASET_ID"]
        table_id = sentiment_json["TABLE_NAME_QUALTRICS"]
        google_creds_value = get_secret("datateam_google_prod")
        google_creds = json.loads(google_creds_value)
        google_credential = Credentials.from_service_account_info(google_creds)

    except json.JSONDecodeError as e:
        logging.info(f"JSON decode error: {e}")
        api_token = (
            survey_id
        ) = server = project_id = dataset_id = table_id = ""
        google_creds = google_credential = None
    except Exception as e:
        logging.info(f"An error occurred: {e}")
        api_token = (
            survey_id
        ) = server = project_id = dataset_id = table_id = ""
        google_creds = google_credential = None

    local_config = {
        "api_token": api_token,
        "survey_id": survey_id,
        "server": server,
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
    }

    if google_creds:
        local_config["credentials"] = Credentials.from_service_account_info(
            google_creds
        )
        local_config[
            "base_url"
        ] = f"https://{local_config['server']}.qualtrics.com/API/v3/surveys/{local_config['survey_id']}/export-responses/"
    # print(local_config)
    return local_config, google_credential


def set_date_range():
    """set date range"""
    try:
        local_end_date = datetime.now(timezone.utc)
        local_start_date = local_end_date - timedelta(days=3)
        end_date_str = local_end_date.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        start_date_str = local_start_date.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        logging.info(
            "Start date set to: %s, End date set to: %s",
            local_start_date,
            local_end_date,
        )
    except Exception as err:
        logging.info("error in set_date_range() %s", err)
    return start_date_str, end_date_str


def request_export(export_start_date, export_end_date, export_config):
    """request data export from qualtrics"""
    try:
        logging.info("Step 1: Requesting export")
        api_token = export_config["api_token"]
        base_url = export_config["base_url"]

        headers = {
            "X-API-TOKEN": api_token,
            "Content-Type": "application/json",
        }

        export_data = {
            "format": "json",
            "startDate": export_start_date,
            "endDate": export_end_date,
        }

        response = requests.post(
            base_url, headers=headers, json=export_data, timeout=10
        )

        if response.status_code != 200:
            logging.error(
                "Something went wrong. Status Code: %s", response.status_code
            )
            logging.error("Response: %s", response.json())
            exit()

        export_progress_id = response.json()["result"]["progressId"]

    except Exception as err:
        logging.info("error in request_export() %s", err)

    return export_progress_id


def check_progress(check_progress_id, check_config):
    """checking the progress of data export"""
    try:
        base_url = check_config["base_url"]
        headers = {"X-API-TOKEN": check_config["api_token"]}
        logging.info("Step 2: Checking progress")

        while True:
            progress_url = f"{base_url}{check_progress_id}"
            progress_response = requests.get(
                progress_url, headers=headers, timeout=10
            )
            check_status = progress_response.json()["result"]["status"]

            if check_status == "complete":
                check_file_id = progress_response.json()["result"]["fileId"]
                return check_status, check_file_id

    except Exception as err:
        logging.info("error in check_progress() %s", err)


def download_and_read_zip(dl_file_id, download_config):
    """download the export from qualtrics"""
    try:
        base_url = download_config["base_url"]
        headers = {"X-API-TOKEN": download_config["api_token"]}
        download_url = f"{base_url}{dl_file_id}/file"

        logging.info("Step 3: Download and read the ZIP file into DataFrame")

        download_response = requests.get(
            download_url, headers=headers, timeout=10
        )

        zip_file = zipfile.ZipFile(BytesIO(download_response.content))
        json_file_name = next(
            (name for name in zip_file.namelist() if name.endswith(".json")),
            None,
        )

        if json_file_name is None:
            logging.error("No JSON file found in the ZIP archive.")
            exit()

        with zip_file.open(json_file_name) as file:
            dataframe = pd.read_json(file)

    except Exception as err:
        logging.info("error in download_and_read_zip() %s", err)

    return dataframe, len(dataframe)


def load_into_bigquery(bq_df, bq_config, google_cred):
    """load into bigquery"""
    try:
        project_id = bq_config["project_id"]
        dataset_id = bq_config["dataset_id"]
        table_id = bq_config["table_id"]

        logging.info("Step 4: Decode the nested JSON and load into BigQuery")

        bq_df["responses"] = bq_df["responses"].apply(json.dumps)
        expanded_df = pd.json_normalize(bq_df["responses"].apply(json.loads))
        final_df = pd.concat(
            [bq_df.drop(["responses"], axis=1), expanded_df], axis=1
        )
        final_df.columns = [col.replace(".", "_") for col in final_df.columns]

        for col in final_df.columns:
            if final_df[col].apply(type).eq(list).any():
                final_df[col] = final_df[col].apply(json.dumps)

        for col in final_df.columns:
            final_df[col] = final_df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, dict) else x
            )

        if "values_category" in final_df.columns:
            final_df["values_category"] = final_df["values_category"].apply(
                json.dumps
            )

    except Exception as err:
        logging.info("error in first half of load_into_bigquery() %s", err)

    try:
        client = bigquery.Client(
            project=bq_config["project_id"], credentials=google_cred
        )
        query = (
            f"SELECT responseId FROM `{project_id}.{dataset_id}.{table_id}`"
        )
        query_job = client.query(query)
        existing_response_ids = [row.responseId for row in query_job]
        final_df = final_df[
            ~final_df["responseId"].isin(existing_response_ids)
        ]
        to_gbq(
            final_df,
            f"{dataset_id}.{table_id}",
            project_id=project_id,
            if_exists="append",
            credentials=google_cred,
        )
        logging.info("Data has been successfully loaded into BigQuery.")
    except Exception as exp_error:
        logging.error("Failed in post-processing: %s", exp_error)
        exit(1)


def qualtrics_run():
    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials from Google Cloud SDK",
    )
    logger("qualtrics")
    config, google_cred = initialize_config()
    start_date, end_date = set_date_range()
    try:
        progress_id = request_export(start_date, end_date, config)
        status, file_id = check_progress(progress_id, config)
        if status == "complete":
            df, df_length = download_and_read_zip(file_id, config)
            logging.info(
                "The length of the downloaded in report from download_and_read_zip() is: %s",
                df_length,
            )
            load_into_bigquery(df, config, google_cred)
        else:
            logging.error("Export did not complete.")
    except Exception as error:
        logging.error("An error occurred: %s", error)
