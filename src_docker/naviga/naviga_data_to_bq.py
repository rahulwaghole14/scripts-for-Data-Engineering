"""
For loading naviga data via API call directly to big query
"""
import logging
import io
import csv
from typing import get_origin, get_args, Union
import json
import requests
import time
from datetime import datetime
from pydantic import ValidationError
from common.aws.aws_secret import get_secret
from common.bigquery.bigquery import (
    pydantic_model_to_bq_schema,
    load_data_to_bigquery_truncate,
    create_bigquery_client,
)
from .validator.data_validator_base import naviga_data
from .logger import logger

# Load environment variables
logger("naviga")

# Setup logger for logging info and errors
logging.basicConfig(level=logging.INFO)

# Get secrets
naviga_secrets_json = get_secret("datateam_naviga_api")
naviga_secrets = json.loads(naviga_secrets_json)

google_creds = get_secret("datateam_google_cred_prod_base64")
google_creds = json.loads(google_creds)
google_creds = google_creds.get("GOOGLE_CLOUD_CRED_BASE64")

api_url = naviga_secrets.get("api_naviga")
dataset_id = "adw_stage"
table_id_base = naviga_secrets.get("table_id_naviga_main")


def extract_base_type(type_hint):
    origin = get_origin(type_hint) or type_hint
    if origin is Union:
        # Handle Optional or other Unions; assumes the first type is the primary type
        return get_args(type_hint)[0]
    return origin


def fetch_and_validate_data(
    api_url, retries=3, log_interval=60, chunk_size=1024 * 1024
):
    """
    Fetches data from the API in batches, validates it using the Pydantic model,
    and logs the progress of the request at specified intervals.
    """
    try:
        logging.info("Starting to connect to API URL")
        response = requests.get(api_url, stream=True, timeout=20)
        if response.status_code == 200:
            logging.info("Data fetched successfully from API.")

            total_size = int(response.headers.get("content-length", 0))
            stream_content = io.BytesIO()

            start_time = time.time()
            last_log_time = start_time
            downloaded_size = 0

            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    stream_content.write(chunk)
                    downloaded_size += len(chunk)
                    current_time = time.time()

                    if current_time - last_log_time >= log_interval:
                        elapsed_time = current_time - start_time
                        download_speed = (
                            downloaded_size / elapsed_time / 1024
                        )  # in KB/s
                        logging.info(
                            f"Downloaded {downloaded_size / (1024 * 1024):.2f} MiB at {download_speed:.2f} KB/s"
                        )
                        last_log_time = current_time

            stream_content.seek(0)

            validated_data = []
            for attempt in range(retries):
                try:
                    stream_content.seek(0)
                    decoded_content = stream_content.read().decode("utf-8")
                    reader = csv.DictReader(io.StringIO(decoded_content))

                    for item in reader:
                        try:
                            validated_model = naviga_data(**item)
                            validated_data.append(validated_model.model_dump())
                        except ValidationError as e:
                            logging.error(f"Validation error: {e.json()}")
                    break
                except UnicodeDecodeError as e:
                    logging.error(f"Unicode decode error: {e}")
                    if attempt < retries - 1:
                        logging.info(f"Retrying... ({attempt + 1}/{retries})")
                    else:
                        logging.error(
                            "Max retries reached. Failed to decode data."
                        )
                        return None, None

            schema = pydantic_model_to_bq_schema(naviga_data)

            return validated_data, schema
        else:
            logging.error(f"Failed to fetch data: HTTP {response.status_code}")
            return None, None
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception: {e}")
        return None, None
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        return None, None


def naviga_run():
    """
    fetch data from naviga api and upload to bigquery
    """

    data, schema = fetch_and_validate_data(api_url)
    if data is not None:
        logging.info(
            "DataFrame fetched and validated successfully. Downloaded Data length: %s",
            len(data),
        )

        client = create_bigquery_client(google_creds)

        # Upload to BigQuery using the dynamically generated schema
        current_date = datetime.now().strftime("%Y%m%d")
        table_id = f"naviga__adrevenue_bq_{current_date}"  # Adding bq to differ the new tables with the current ones

        load_data_to_bigquery_truncate(
            client, dataset_id, table_id, data, schema
        )

    else:
        logging.error(
            "No data fetched or validation failed; upload to BigQuery skipped."
        )
