import json
import os
import warnings
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from pandas_gbq import to_gbq
from common.aws.aws_secret import get_secret
import logging

# Import the logger function
from qualtrics.logger import logger

def initialize_config(survey_id, survey_name):
    """setup config"""
    load_dotenv()
    try:
        secret_value = get_secret("datateam_qualtrics_api")
        secret_json = json.loads(secret_value)
        api_token = secret_json.get("QUALTRICS_API_TOKEN", "")
        server = secret_json.get("QUALTRICS_DATA_CENTER", "")
        
        google_creds_value = get_secret("datateam_google_prod")
        google_creds = json.loads(google_creds_value)
        project_id = google_creds.get("project_id", "")
        dataset_id = 'cdw_stage'
        table_id = f'qualtrics__{survey_name.lower()}_responses'
        google_credential = Credentials.from_service_account_info(google_creds)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        api_token = server = project_id = dataset_id = table_id = ""
        google_creds = google_credential = None
    except Exception as e:
        print(f"An error occurred: {e}")
        api_token = server = project_id = dataset_id = table_id = ""
        google_creds = google_credential = None

    local_config = {
        "api_token": api_token,
        "survey_id": survey_id,
        "survey_name": survey_name,
        "server": server,
        "project_id": project_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
    }

    if google_creds and api_token and server and project_id:
        local_config["credentials"] = google_credential
        local_config["base_url"] = f"https://{local_config['server']}.qualtrics.com/API/v3/surveys/{local_config['survey_id']}/export-responses/"
    else:
        logging.error("Error: Missing required configuration values.")
    
    print(local_config)
    return local_config, google_credential

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
        export_progress_id = None

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
        return None, None

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
        dataframe = pd.DataFrame()

    return dataframe, len(dataframe)

def create_bigquery_table_if_not_exists(client, project_id, dataset_id, table_id, schema):
    """Create BigQuery table if it doesn't exist"""
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_table(table_ref)
        logging.info("Table %s.%s.%s already exists.", project_id, dataset_id, table_id)
    except Exception as e:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logging.info("Table %s.%s.%s created.", project_id, dataset_id, table_id)

def load_into_bigquery(bq_df, bq_config, google_cred):
    """Load into BigQuery"""
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

        final_df["survey_name"] = bq_config["survey_name"]
        final_df["survey_id"] = bq_config["survey_id"]

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

        schema = [
            bigquery.SchemaField(name="responseId", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="survey_name", field_type="STRING", mode="NULLABLE"),
            bigquery.SchemaField(name="survey_id", field_type="STRING", mode="NULLABLE"),
            # Add other fields here based on your DataFrame columns
        ]

        for column_name, dtype in final_df.dtypes.items():
            if column_name not in ["responseId", "survey_name", "survey_id"]:  # These are already added manually
                if dtype == 'object':
                    field_type = 'STRING'
                elif dtype in ['int64', 'float64']:
                    field_type = 'FLOAT' if dtype == 'float64' else 'INTEGER'
                elif dtype == 'bool':
                    field_type = 'BOOLEAN'
                elif dtype == 'datetime64[ns]':
                    field_type = 'TIMESTAMP'
                else:
                    field_type = 'STRING'
                schema.append(bigquery.SchemaField(column_name, field_type))

        client = bigquery.Client(
            project=bq_config["project_id"], credentials=google_cred
        )
        
        # Fetch existing table schema
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            existing_table = client.get_table(table_ref)
            existing_schema = {field.name: field.field_type for field in existing_table.schema}
        except Exception as e:
            existing_schema = {}
            create_bigquery_table_if_not_exists(client, project_id, dataset_id, table_id, schema)

        # Identify new columns
        new_columns = [field for field in schema if field.name not in existing_schema]

        # Print out schema differences
        logging.info("New columns to add: %s", new_columns)

        # Add new columns if needed
        if new_columns:
            table = bigquery.Table(table_ref, schema=existing_table.schema + new_columns)
            client.update_table(table, ["schema"])
            logging.info("Table schema updated with new columns.")

    except Exception as err:
        logging.info("error in first half of load_into_bigquery() %s", err)

    # Log DataFrame schema and BigQuery table schema for comparison
    logging.info("DataFrame schema: %s", final_df.dtypes.to_dict())
    try:
        existing_table = client.get_table(table_ref)
        logging.info("BigQuery table schema: %s", {field.name: field.field_type for field in existing_table.schema})
    except Exception as e:
        logging.error("Failed to retrieve BigQuery table schema: %s", e)

    # Convert DataFrame columns to match BigQuery table schema
    for column, dtype in final_df.dtypes.items():
        if dtype.name.upper() != existing_schema.get(column, dtype.name.upper()):
            if existing_schema.get(column) == 'STRING':
                final_df[column] = final_df[column].astype(str)
            elif existing_schema.get(column) == 'INTEGER':
                final_df[column] = final_df[column].astype(int)
            elif existing_schema.get(column) == 'FLOAT':
                final_df[column] = final_df[column].astype(float)

    try:
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





def qualtrics_run(survey_id, survey_name, start_date_str, end_date_str):
    warnings.filterwarnings(
        "ignore",
        "Your application has authenticated using end user credentials from Google Cloud SDK",
    )
    logger("qualtrics")
    config, google_cred = initialize_config(survey_id, survey_name)
    if not config["api_token"] or not config["base_url"] or not config["project_id"]:
        logging.error("Missing configuration values. Exiting.")
        return
    try:
        progress_id = request_export(start_date_str, end_date_str, config)
        if not progress_id:
            logging.error("Failed to request export.")
            return
        status, file_id = check_progress(progress_id, config)
        if status == "complete":
            df, df_length = download_and_read_zip(file_id, config)
            logging.info(
                "The length of the downloaded report from download_and_read_zip() is: %s",
                df_length,
            )
            load_into_bigquery(df, config, google_cred)
        else:
            logging.error("Export did not complete.")
    except Exception as error:
        logging.error("An error occurred: %s", error)
