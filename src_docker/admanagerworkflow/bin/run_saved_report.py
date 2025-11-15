"""Module to run reports using the Google Ad Manager API.

This module contains functions to interact with the Google Ad Manager API,
to run saved reports based on specified criteria, and to process the results.
The reports can be run with or without a specified date range, and the data
is initially fetched in CSV format before being converted into a pandas DataFrame.

Functions:
    run_report: Run a report with a specified date range.
    run_report_no_date: Run a report without specifying a date range.
"""
import os
import logging
import time
import tempfile
import csv
from googleads import ad_manager, errors
import pandas as pd
from common.validation.validators import validate_list_of_dicts_serialised

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def preprocess_data(dataframe):
    """preprocess the data in the dataframe"""

    logging.info("Preprocessing data...")

    # Integer columns
    integer_columns = [
        "ADVERTISER_ID",
        "DEVICE_CATEGORY_ID",
        "LINE_ITEM_ID",
        "ORDER_ID",
        "REGION_CRITERIA_ID",
        "COUNTRY_CRITERIA_ID",
        "TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS",
        "TOTAL_LINE_ITEM_LEVEL_CLICKS",
        "HOUR",
        "TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS",
        "TOTAL_LINE_ITEM_LEVEL_WITH_CPD_AVERAGE_ECPM",
        "SELL_THROUGH_FORECASTED_IMPRESSIONS",
        "SELL_THROUGH_RESERVED_IMPRESSIONS",
        "CONTENT_ID",
    ]
    for col in integer_columns:
        placeholder = (
            -2
        )  # '-2' is just a number indicates NaN, Adjust placeholder if needs
        if col in dataframe.columns:
            dataframe[col] = (
                pd.to_numeric(dataframe[col], errors="coerce")
                .fillna(placeholder)
                .astype("int64")
            )
        else:
            logger.warning("Column %s not found in DataFrame", col)

    # String columns
    string_columns = [
        "ADVERTISER_NAME",
        "DEVICE_CATEGORY_NAME",
        "LINE_ITEM_NAME",
        "ORDER_NAME",
        "REGION_NAME",
        "COUNTRY_NAME",
        "DATE",
        "ORDER_END_DATE_TIME",
        "ORDER_START_DATE_TIME",
        "LINE_ITEM_GOAL_QUANTITY",
        "LINE_ITEM_START_DATE_TIME",
        "LINE_ITEM_END_DATE_TIME",
        "CONTENT_NAME",
        "CONTENT_CMS_NAME",
        "CONTENT_CMS_VIDEO_ID",
    ]
    for col in string_columns:
        if col in dataframe.columns:
            dataframe[col] = dataframe[col].astype(str)
        else:
            logger.warning("Column %s not found in DataFrame", col)

    # Float columns
    float_columns = ["TOTAL_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS_RATE"]
    for col in float_columns:
        if col in dataframe.columns:
            dataframe[col] = pd.to_numeric(dataframe[col], errors="coerce")
        else:
            logger.warning("Column %s not found in DataFrame", col)

    logger.info("Data types in the DataFrame after preprocessing:")
    for col in dataframe.columns:
        logger.info("Column: %s, Type: %s", col, dataframe[col].dtype)

    return dataframe


def clean_column_names(dataframe):
    """
    Remove everything before and including the dot in each column name
    """

    dataframe.columns = [col.split(".")[-1] for col in dataframe.columns]
    return dataframe


def run_report(
    client,
    report_service,
    version,
    saved_query_id,
    start_year=None,
    start_month=None,
    start_day=None,
    end_year=None,
    end_month=None,
    end_day=None,
):
    """
    Runs a report in Google Ad Manager for a specified query ID within a date range.

    Args:
        client: The Ad Manager client object.
        report_service: The report service object from the Ad Manager API.
        version (str): The version of the Ad Manager API to use.
        saved_query_id (int): The ID of the saved query to run.
        start_date (date): The start date for the report range.
        end_date (date): The end date for the report range.

    Returns:
        pandas.DataFrame: A DataFrame containing the report data.
    """
    start_time = time.time()
    # Create a statement to select the saved query.
    statement = (
        ad_manager.StatementBuilder(version=version)
        .Where("id = :id")
        .WithBindVariable("id", int(saved_query_id))
        .Limit(1)
    )

    # Get the saved query.
    response = report_service.getSavedQueriesByStatement(
        statement.ToStatement()
    )

    if "results" in response and len(response["results"]):
        saved_query = response["results"][0]
        if saved_query["isCompatibleWithApiVersion"]:
            report_job = {"reportQuery": saved_query["reportQuery"]}

            # Set the date range for the report if provided
            if (
                start_year
                and start_month
                and start_day
                and end_year
                and end_month
                and end_day
            ):
                report_job["reportQuery"]["startDate"] = {
                    "year": start_year,
                    "month": start_month,
                    "day": start_day,
                }
                report_job["reportQuery"]["endDate"] = {
                    "year": end_year,
                    "month": end_month,
                    "day": end_day,
                }

            # Initialize a DataDownloader.
            report_downloader = client.GetDataDownloader(version=version)

            try:
                # Run the report and wait for it to finish.
                report_job_id = report_downloader.WaitForReport(report_job)
            except errors.AdManagerReportError as e:
                logger.info("Failed to generate report. Error was: %s", e)
                return pd.DataFrame()  # Return an empty DataFrame on failure

            # Download the report data into a file.
            export_format = "CSV_DUMP"
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                report_downloader = client.GetDataDownloader(version=version)
                report_downloader.DownloadReportToFile(
                    report_job_id,
                    export_format,
                    temp_file,
                    use_gzip_compression=False,
                )
                report_file_path = (
                    temp_file.name
                )  # Store the path of the temporary file

            # Read the report data from the file into a pandas DataFrame.
            dataframe = clean_column_names(pd.read_csv(report_file_path))
            dataframe = preprocess_data(dataframe)

            # Save the DataFrame to a CSV file for debugging purpose
            # dataframe.to_csv('/Users/amber.wang/Downloads/report3.csv', index=False)

            # Delete the temporary file.
            os.remove(report_file_path)

            end_time = time.time()  # Record the end time
            total_time = end_time - start_time  # Calculate total runtime
            logger.info(
                "Report generation completed in %s seconds.",
                format(total_time, ".2f"),
            )

            return dataframe

    else:
        print("No results found.")
        return pd.DataFrame()  # Return an empty DataFrame if no results.


def run_report_validated(
    client,
    report_service,
    version,
    saved_query_id,
    start_year=None,
    start_month=None,
    start_day=None,
    end_year=None,
    end_month=None,
    end_day=None,
    pydantic_data_model=None,
):
    """
    Runs a report in Google Ad Manager for a specified query ID within a date range.
    and validate the data against pydantic model
    return the validated data in a list of dictionaries

    Args:
        client: The Ad Manager client object.
        report_service: The report service object from the Ad Manager API.
        version (str): The version of the Ad Manager API to use.
        saved_query_id (int): The ID of the saved query to run.
        start_date (date): The start date for the report range.
        end_date (date): The end date for the report range.
        pydantic_data_model: The pydantic model to validate the data against
    Returns:
        list: A list of dictionaries containing the report data.
    """
    start_time = time.time()
    # Create a statement to select the saved query.
    statement = (
        ad_manager.StatementBuilder(version=version)
        .Where("id = :id")
        .WithBindVariable("id", int(saved_query_id))
        .Limit(1)
    )

    # Get the saved query.
    response = report_service.getSavedQueriesByStatement(
        statement.ToStatement()
    )

    if "results" in response and len(response["results"]):
        saved_query = response["results"][0]
        if saved_query["isCompatibleWithApiVersion"]:
            report_job = {"reportQuery": saved_query["reportQuery"]}

            # Set the date range for the report if provided
            if (
                start_year
                and start_month
                and start_day
                and end_year
                and end_month
                and end_day
            ):
                report_job["reportQuery"]["startDate"] = {
                    "year": start_year,
                    "month": start_month,
                    "day": start_day,
                }
                report_job["reportQuery"]["endDate"] = {
                    "year": end_year,
                    "month": end_month,
                    "day": end_day,
                }

            # Initialize a DataDownloader.
            report_downloader = client.GetDataDownloader(version=version)

            try:
                # Run the report and wait for it to finish.
                report_job_id = report_downloader.WaitForReport(report_job)
            except errors.AdManagerReportError as e:
                print(f"Failed to generate report. Error was: {e}")
                return pd.DataFrame()  # Return an empty DataFrame on failure

            # Download the report data into a file.
            export_format = "CSV_DUMP"
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                report_downloader = client.GetDataDownloader(version=version)
                report_downloader.DownloadReportToFile(
                    report_job_id,
                    export_format,
                    temp_file,
                    use_gzip_compression=False,
                )
                report_file_path = (
                    temp_file.name
                )  # Store the path of the temporary file

            # Read the report data from the file into a list of dictionaries.
            data_list = []
            with open(report_file_path, mode="r", encoding="utf-8") as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    data_list.append(row)

            # validate the data_list against pydantic model for pca_view
            logger.info("Validating the data against the pydantic model...")
            pca_view_data_validated = validate_list_of_dicts_serialised(
                data_list, pydantic_data_model
            )

            # Save the DataFrame to a CSV file for debugging purpose
            # dataframe.to_csv('/Users/amber.wang/Downloads/report3.csv', index=False)

            # Delete the temporary file.
            os.remove(report_file_path)

            end_time = time.time()  # Record the end time
            total_time = end_time - start_time  # Calculate total runtime
            logger.info(
                "Report generation completed in %s seconds.",
                format(total_time, ".2f"),
            )

            return pca_view_data_validated

    else:
        logger.info("No results found.")
        return []
