"""
gsheets
"""
import logging
import json
import os
from datetime import date, timedelta
import pandas
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()


def keep_last_n_days(df, days):
    """
    keep last n days
    """
    date_filter = pandas.to_datetime(date.today() - timedelta(days=days))
    logging.info("Filtering content starting after: %s", str(date_filter))
    df = df[(df["START DATE"] > date_filter)]

    return df


def get_gsheet_dataframe(spreadsheet_name, head, credentials):
    """
    Get Google Sheet data and return as a DataFrame
    """
    df = None  # Initialize df to handle cases where it might not get set
    try:
        logging.info("set creds in get_gsheet_dataframe()")
        client = gspread.authorize(credentials)
        logging.info("set client in get_gsheet_dataframe()")
        sheet = client.open(spreadsheet_name).sheet1.get_all_records(head=head)
        logging.info("set sheet in get_gsheet_dataframe()")
        df = pandas.DataFrame(data=sheet)
        logging.debug(df)
    except Exception as error:
        logging.info("error in get_gsheet_dataframe(), %s", error)

    return df
