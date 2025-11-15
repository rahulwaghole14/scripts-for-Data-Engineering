"""
read network files and send to bigquery tables
"""

import os
import logging
from datetime import datetime, date

import toml
from a_common.logging.logger import logger
from a_common.bigquery.bigquery import (
    create_bigquery_client,
    load_data_to_bigquery_truncate,
    # load_dataframe_to_bigquery_truncate,
    pydantic_model_to_bq_schema)
from a_common.validation.validators import validate_dataframe

from .readcsvs import read_csv_file
from .validator import (
    # MastheadArticlesByDay,
    # MastheadLoggedInUsers,
    # MastheadDaily,
    Targetstableforpbi,
    Navigatableforpbi,
    Datetable)

logger('networkdrive_to_bigquery',
       os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log'))

# Directory above the current script's directory
root_dir = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))

# Path to the config.toml file
config_file_path = os.path.join(root_dir, 'config.toml')

# Path to your TOML file
FILE_PATH = config_file_path
with open(FILE_PATH, 'r', encoding='utf-8') as toml_file:
    data = toml.load(toml_file)

# google
google_cred = data['google']['cloud_cred_base64']

# drives
drive_datainsights = data['drives']['root_folder_datainsights']
drive_datateam = data['drives']['root_folder_datateam']

# files
masthead_articles_by_day = drive_datainsights + data['datainsights']['masthead_articles_by_day']
masthead_logged_in_users = drive_datainsights + data['datainsights']['masthead_logged_in_users']
masthead_daily = drive_datainsights + data['datainsights']['masthead_daily']

targetstableforpbi = drive_datateam + data['datateam']['targetstableforpbi']
navigatableforpbi = drive_datateam + data['datateam']['navigatableforpbi']
datetable = drive_datateam + data['datateam']['datetable']


if __name__ == "__main__":

    try:

        # read from source
        # datainsights_masthead_articles_by_day = read_csv_file(masthead_articles_by_day)
        # datainsights_masthead_logged_in_users = read_csv_file(masthead_logged_in_users)
        # datainsights_masthead_daily = read_csv_file(masthead_daily)
        datateam_targetstableforpbi = read_csv_file(targetstableforpbi)
        datateam_navigatableforpbi = read_csv_file(navigatableforpbi)
        datateam_datetable = read_csv_file(datetable)

        # validate
        # masthead_articles_by_day_val = validate_dataframe(
        #     datainsights_masthead_articles_by_day, MastheadArticlesByDay)
        # masthead_logged_in_users_val = validate_dataframe(
        #     datainsights_masthead_logged_in_users, MastheadLoggedInUsers)
        # masthead_daily_val = validate_dataframe(
        #     datainsights_masthead_daily, MastheadDaily)
        targetstableforpbi_val = validate_dataframe(
            datateam_targetstableforpbi, Targetstableforpbi)
        navigatableforpbi_val = validate_dataframe(
            datateam_navigatableforpbi, Navigatableforpbi)
        datateam_datetable_val = validate_dataframe(
            datateam_datetable, Datetable)


        # preprocessing records to convert date/datetime to string
        def preprocess_records(records):
            ''' preprocess records dates '''
            for record in records:
                for key, value in record.items():
                    if isinstance(value, (datetime, date)):
                        record[key] = value.isoformat()
            return records

        # masthead_articles_by_day_val = preprocess_records(masthead_articles_by_day_val)
        # masthead_logged_in_users_val = preprocess_records(masthead_logged_in_users_val)
        # masthead_daily_val = preprocess_records(masthead_daily_val)
        targetstableforpbi_val = preprocess_records(targetstableforpbi_val)
        navigatableforpbi_val = preprocess_records(navigatableforpbi_val)
        datateam_datetable_val = preprocess_records(datateam_datetable_val)

        # generate schema
        # masthead_articles_by_day_schema = pydantic_model_to_bq_schema(MastheadArticlesByDay)
        # masthead_logged_in_users_schema = pydantic_model_to_bq_schema(MastheadLoggedInUsers)
        # masthead_daily_schema = pydantic_model_to_bq_schema(MastheadDaily)
        targetstableforpbi_schema = pydantic_model_to_bq_schema(Targetstableforpbi)
        navigatableforpbi_schema = pydantic_model_to_bq_schema(Navigatableforpbi)
        datateam_datetable_schema = pydantic_model_to_bq_schema(Datetable)

        # load to bigquery
        client = create_bigquery_client(google_cred)

        # load dataandinights
        # load_data_to_bigquery_truncate(
        #     client, 'cdw_stage', 'network__masthead_articles_by_day',
        #     masthead_articles_by_day_val,
        #     masthead_articles_by_day_schema)
        # load_data_to_bigquery_truncate(
        #     client, 'cdw_stage', 'network__masthead_logged_in_users',
        #     masthead_logged_in_users_val,
        #     masthead_logged_in_users_schema)
        # load_data_to_bigquery_truncate(
        #     client, 'cdw_stage', 'network__masthead_daily',
        #     masthead_daily_val,
        #     masthead_daily_schema)

        # load datateam
        load_data_to_bigquery_truncate(
            client, 'cdw_stage', 'network__targetstableforpbi',
            targetstableforpbi_val,
            targetstableforpbi_schema)
        load_data_to_bigquery_truncate(
            client, 'cdw_stage', 'network__navigatableforpbi',
            navigatableforpbi_val,
            navigatableforpbi_schema)
        load_data_to_bigquery_truncate(
            client, 'cdw_stage', 'network__datetable',
            datateam_datetable_val,
            datateam_datetable_schema)

    except Exception as err:
        logging.error("Error in main: %s", err)
        raise err
