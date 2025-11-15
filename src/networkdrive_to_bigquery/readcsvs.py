"""
read csv
"""

import os
import logging
import re
import pandas as pd

def read_csv_file(file_path: str):
    """
    Read a CSV file into a pandas DataFrame
    """

    absolute_csv_file_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), file_path)

    # trim and lowercase the file name and remove spaces
    csv_file_basename = "network__" + os.path.splitext(
        os.path.basename(absolute_csv_file_path))[0].replace(' ', '_').strip().lower()

    try:
        if os.path.exists(absolute_csv_file_path):

            df = pd.read_csv(absolute_csv_file_path, low_memory=False)
            df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col) for col in df.columns]
            logging.info(
                "Successfully read CSV file %s into DataFrame. Number of rows: %s",
                csv_file_basename, len(df))
            return df

        logging.error("CSV file not found at %s", absolute_csv_file_path)
        return None
    except UnicodeDecodeError:
        logging.info("Error reading CSV file %s in utf-8", absolute_csv_file_path)
        try:
            df = pd.read_csv(absolute_csv_file_path, low_memory=False, encoding='ISO-8859-1')
            df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col) for col in df.columns]
            logging.info(
                "Successfully read CSV file %s into DataFrame. Number of rows: %s",
                csv_file_basename, len(df))
            return df
        except UnicodeDecodeError:
            logging.info("Error reading CSV file %s in ISO-8859-1", absolute_csv_file_path)
            try:
                df = pd.read_csv(absolute_csv_file_path, low_memory=False, encoding='latin1')
                df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col) for col in df.columns]
                logging.info(
                    "Successfully read CSV file %s into DataFrame. Number of rows: %s",
                    csv_file_basename, len(df))
                return df
            except UnicodeDecodeError:
                logging.info("Error reading CSV file %s in latin1", absolute_csv_file_path)
                try:
                    df = pd.read_csv(absolute_csv_file_path, low_memory=False, encoding='cp1252')
                    df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', col) for col in df.columns]
                    logging.info(
                    "Successfully read CSV file %s into DataFrame. Number of rows: %s",
                    csv_file_basename, len(df))
                    return df
                except UnicodeDecodeError:
                    logging.error("Error reading CSV file %s in cp1252", absolute_csv_file_path)
                    return None
