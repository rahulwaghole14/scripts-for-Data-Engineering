""" For updating showcase_plus data monthly"""
import os
import re
import json
import sys
from datetime import datetime, timedelta, timezone
from common.aws.aws_secret import get_secret
from common.logging.logger2 import get_logger
from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from google.oauth2 import service_account
from googleapiclient.discovery import build
from pydantic import ValidationError
from .data_validation import SheetDataRow

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


logger = get_logger("get showcase_plus monthly data")

airbite_google_creds_json = get_secret("datateam_airbyte_prod")
google_creds_json = get_secret("datateam_google_prod")


def create_sheets_client():
    google_cred_json = airbite_google_creds_json
    if google_cred_json:
        try:
            credentials_info = json.loads(google_cred_json)
            credentials = (
                service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=[
                        "https://www.googleapis.com/auth/spreadsheets.readonly"
                    ],
                )
            )
            service = build(
                "sheets", "v4", credentials=credentials, cache_discovery=False
            )
            logger.info("Created Google Sheets client")
            return service
        except Exception as error:
            logger.error(f"Failed to create Google Sheets client: {error}")
    else:
        logger.error("No credentials found in environment variables")
    return None


def get_previous_month():
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.strftime("%b")  # Output will be 'Apr', 'May', etc.


def get_sheets_with_name(service, spreadsheet_id, keyword, month):
    if service:
        try:
            sheet_metadata = (
                service.spreadsheets()
                .get(spreadsheetId=spreadsheet_id)
                .execute()
            )
            sheets = sheet_metadata.get("sheets", [])
            target_sheets = [
                sheet["properties"]["title"]
                for sheet in sheets
                if keyword in sheet["properties"]["title"]
                and month in sheet["properties"]["title"]
            ]
            return target_sheets
        except Exception as error:
            logger.error(f"Failed to fetch sheets: {error}")
    return []  # Return an empty list instead of None


def read_sheet_data(service, spreadsheet_id, sheet_name):
    try:
        range_name = f"{sheet_name}"
        result = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        values = result.get("values", [])
        if not values:
            logger.info(f"No data found in {sheet_name}")
            return [], []

        headers = values[0]
        try:
            # Ensure there is a header for 'Notes' and it can be found in the first row
            start_idx = 1  # Second column index
            end_idx = headers.index("Notes") + 1
            if "Notes" not in headers:
                raise ValueError("Notes column not found")
        except ValueError as e:
            logger.error(
                f"Error finding 'Notes' column in {sheet_name}: {str(e)}"
            )
            return [], []

        sliced_headers = headers[start_idx:end_idx]
        # Instead of filtering out rows based on length, just slice up to end_idx and handle missing values gracefully
        sliced_data = [
            row[start_idx : end_idx if end_idx <= len(row) else len(row)]
            for row in values[1:]
        ]
        logger.info(f"sliced_headers from {sheet_name}: {sliced_headers}")
        logger.info(
            f"First few rows data from {sheet_name}: {values[1:3]}"
        )  # Log first five rows to inspect the actual data
        logger.info(
            f"Data from sheet {sheet_name}: Retrieved {len(sliced_data)} rows with headers from {sliced_headers[0]} to Notes."
        )
        return sliced_headers, sliced_data
    except Exception as error:
        logger.error(f"Error reading data from sheet {sheet_name}: {error}")
        return [], []


def infer_schema(headers, data):
    if not data:
        return []

    schema = []
    for i, header in enumerate(headers):
        sanitized_header = sanitize_header(
            header, i
        )  # Apply the sanitized logic
        col_type = "STRING"  # Default type
        # if all(row[i].isdigit() for row in data if len(row) > i):
        #     col_type = "INTEGER"
        # elif all(is_float(row[i]) for row in data if len(row) > i):
        #     col_type = "FLOAT"
        # elif all(row[i].lower() in ['true', 'false'] for row in data if len(row) > i):
        #     col_type = "BOOLEAN"

        schema.append(bigquery.SchemaField(sanitized_header, col_type))
    schema.append(bigquery.SchemaField("record_load_dts", "TIMESTAMP"))
    return schema


def is_float(value):
    """Helper function to check if a string can be interpreted as a float."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def create_bigquery_client():
    google_cred_json = google_creds_json
    if google_cred_json:
        try:
            credentials_info = json.loads(google_cred_json)
            credentials = (
                service_account.Credentials.from_service_account_info(
                    credentials_info
                )
            )
            client = bigquery.Client(
                credentials=credentials, project=credentials_info["project_id"]
            )
            logger.info("Created BigQuery client")
            return client
        except Exception as error:
            logger.error(f"Failed to create BigQuery client: {error}")
    else:
        logger.error("No credentials found in environment variables")
    return None


def create_bigquery_table(client, dataset_id, table_id, schema):
    if client:
        try:
            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)  # Attempts to create the table
            logger.info(
                f"Created BigQuery table {table_id} in dataset {dataset_id}"
            )
            return table
        except Conflict:
            logger.info(
                f"Table {table_id} already exists in dataset {dataset_id}. Proceeding to next steps."
            )
            return client.get_table(table_ref)  # Return the existing table
        except Exception as error:
            logger.error(f"Error creating table {table_id}: {str(error)}")
            return None


def upload_data_to_bigquery(client, dataset_id, table_id, data):
    if client and data:
        table_ref = client.dataset(dataset_id).table(table_id)
        try:
            errors = client.insert_rows_json(table_ref, data)
            if errors:
                logger.error(f"Errors returned from data upload: {errors}")
            else:
                logger.info(f"Data uploaded successfully to {table_id}")
        except Exception as e:
            logger.error(
                f"Failed to upload data to table {table_id}: {str(e)}"
            )


def sanitize_table_name(sheet_name):
    """Sanitize the table name to conform to BigQuery naming conventions and rearrange to year_res_comm_month format."""

    # sheet_name format is something like '2024 Feb-Res&Comm'
    parts = re.split(r"[-\s]", sheet_name)  # Split on spaces and dashes

    # Expected to split into ['2024', 'Feb', 'Res&Comm']
    if len(parts) >= 3:
        year = parts[0]
        month = parts[1]
        keyword = parts[2]

        # Remove non-alphanumeric characters from parts, except underscores
        sanitized_parts = [
            re.sub(r"[^\w]", "", part) for part in [year, keyword, month]
        ]

        # Rearrange and join parts to form the new table name
        new_name = (
            f"{sanitized_parts[0]}_res_comm_{sanitized_parts[2].lower()}"
        )
        return new_name
    else:
        # Fallback if the name does not match the expected format
        logger.error(
            "Unexpected sheet name format, check the naming convention used."
        )
        return None


def sanitize_header(header, index):
    """Remove disallowed characters, replace specific characters as specified,
    and ensure the header conforms to BigQuery field name requirements, with unique fallbacks."""
    if not header.strip() or header.strip() == ",":
        header = f"Column{index+1}"  # Assign a unique name based on position

    # Specific replacements before general sanitization to prevent replacing newly introduced underscores

    header = header.replace("`", "X")
    header = header.replace("_", "Y")
    header = header.replace(".", "Z")

    # Replace all remaining non-alphanumeric characters (excluding Y and Z we just introduced) with underscore
    header = re.sub(r"[^\wYZ]+", "_", header)

    # Truncate to the maximum length allowed by BigQuery
    return header[:300]


def format_and_validate_data(headers, data):
    formatted_data = []
    errors = []
    current_timestamp = datetime.now(
        timezone.utc
    ).isoformat()  # Compute timestamp once for all rows for consistency
    sanitized_headers = [
        sanitize_header(header, i) for i, header in enumerate(headers)
    ]  # Sanitize headers

    for row in data:
        try:
            # Create a dictionary using sanitized headers and validate it with the Pydantic model
            row_dict = dict(zip(sanitized_headers, row))
            validated_row = SheetDataRow(**row_dict)
            # Convert the validated row to a dictionary and append the timestamp
            data_row = validated_row.model_dump()
            data_row["record_load_dts"] = current_timestamp
            formatted_data.append(data_row)
        except ValidationError as e:
            errors.append(e.json())
    if errors:
        logger.error("Validation errors occurred: " + "; ".join(errors))
    return formatted_data


def showcase_run():
    SPREADSHEET_ID = "1zGtSN8Bj8u_U35gHiB-50kREARyL_fFGEAmuAbJRE8I"
    sheets_service = create_sheets_client()
    bigquery_client = create_bigquery_client()

    if not sheets_service or not bigquery_client:
        logger.error("Failed to create Google Sheets or BigQuery client.")
    else:
        previous_month = get_previous_month()
        print(f"Processing data for the month: {previous_month}")
        key_words = ["Res&Comm", "Res & Comm"]

        for key_word in key_words:
            month_specific_sheets = get_sheets_with_name(
                sheets_service, SPREADSHEET_ID, key_word, previous_month
            )
            for sheet in month_specific_sheets:
                headers, data = read_sheet_data(
                    sheets_service, SPREADSHEET_ID, sheet
                )
                if data:
                    SCHEMA = infer_schema(headers, data)
                    DATASET_ID = "adw_stage"
                    sanitized_name = sanitize_table_name(sheet)
                    TABLE_ID = f"{sanitized_name.lower()}"

                    try:
                        table = create_bigquery_table(
                            bigquery_client, DATASET_ID, TABLE_ID, SCHEMA
                        )
                        if table:
                            formatted_data = format_and_validate_data(
                                headers, data
                            )
                            upload_data_to_bigquery(
                                bigquery_client,
                                DATASET_ID,
                                TABLE_ID,
                                formatted_data,
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to process sheet {sheet}: {str(e)}"
                        )


if __name__ == "__main__":
    showcase_run()
