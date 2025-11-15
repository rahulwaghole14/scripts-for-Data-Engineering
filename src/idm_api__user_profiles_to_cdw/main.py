''' main '''
# pylint: disable=import-error

import datetime
import logging
import os
import time

from typing import Optional
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.util import deprecations
from api_idm.batch_deduplicate import batch_get_users
from api_idm.clean_data import (
    map_columns,
    read_list_of_dicts,
    remove_duplicates,
    replace_non_utf8)
from api_idm import run_stored_procedure, create_uuid
from dotenv import load_dotenv
from logger import logger
from pydantic import BaseModel, constr, EmailStr, ValidationError

logger('idm')

# Load the environment variables from .env file
deprecations.SILENCE_UBER_WARNING = True
load_dotenv()

# filter params
now = datetime.datetime.now(datetime.UTC)
delta = datetime.timedelta(hours=48)
before = now - delta
timestamp = before.strftime("%Y-%m-%dT%H:%M:%SZ")

# set mode 0 = get data between dates,
# 1 = get all data from date,
# 2 = get data between dates and filter on id,
MODE = 1

# filter on ids
# id le "93888" and id ge "83888"
START_ID = 400000
END_ID = 500000

# START_DATE = timestamp # 24 hours before now utc
END_DATE = None
START_DATE = timestamp
# START_DATE = "2023-08-04T00:00:00Z"
# END_DATE = "2022-11-19T00:00:00Z"
COUNT = 1000
# COUNT = 200 # batch sizes between the dates


TABLE_NAME = "drupal__user_profiles"
SCHEMA_NAME = "idm"

# uuid namespace
UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")

# API endpoint and credentials
# API_URL = "https://account-management-stage.staging.nebula-drupal.hexa.co.nz"
API_URL = "https://accounts.hexa.co.nz"
username = os.environ.get("API_USERNAME")
password = os.environ.get("API_PASSWORD")
apiauth = os.environ.get("API_AUTH")

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")

# Set up the database connection
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={DRIVER}&TrustServerCertificate=yes"
engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)

MAX_RETRIES = 5
RETRY_DELAY = 15.0  # delay between retries in seconds

class UserProfile(BaseModel):
    user_id: Optional[int]
    adobe_id: Optional[str]
    subscriber_id: Optional[str]
    username: Optional[str]
    record_source: Optional[constr(max_length=255)]
    active: Optional[str]  # or Boolean if it's a boolean value
    country: Optional[str]
    postcode: Optional[str]
    street_address: Optional[str]
    display_name: Optional[str]
    emails_primary: Optional[str]  # Adjust if this is a boolean
    emails_type: Optional[str]
    email: Optional[EmailStr]
    resource_type: Optional[str]
    last_name: Optional[str]
    first_name: Optional[str]
    phone_type: Optional[str]
    contact_phone: Optional[str]
    timezone: Optional[str] = None
    user_consent: Optional[str]
    email_verified: Optional[str]  # Adjust if this is a boolean
    gender: Optional[str]
    mobile_verified: Optional[str]  # Adjust if this is a boolean
    date_of_birth: Optional[datetime.datetime]
    created_date: Optional[datetime.datetime]
    last_modified: Optional[datetime.datetime]
    verified_date: Optional[datetime.datetime]
    mobile_verified_date: Optional[datetime.datetime]
    year_of_birth: Optional[datetime.datetime]
    city_region: Optional[str]
    locality: Optional[str]
    record_load_dts_utc: Optional[datetime.datetime]
    hash_diff: Optional[str]
    newsletter_subs: Optional[str]  # JSON or string, adjust as needed
    marketing_id: Optional[str]
    marketing_id_email: Optional[str]

def validate_data(dataframe):
    valid_data = []
    for index, row in dataframe.iterrows():
        try:
            # Convert row to dictionary and validate
            user_profile = UserProfile(**row.to_dict())
            valid_data.append(user_profile.dict())
        except ValidationError as e:
            # Handle or log validation errors
            logging.info(f"Validation error at row {index}: {e}")

    # Optionally, convert the list of dictionaries back to a DataFrame
    return pd.DataFrame(valid_data)


def execute_sp_with_retry(engine, schema_name, sp_name):
    ''' execute sp with retry '''
    for i in range(MAX_RETRIES):
        logging.info(f"Attempt {i + 1} to execute {sp_name}...")
        try:
            run_stored_procedure.stored_procedure(engine, schema_name, SP_NAME=sp_name)
            logging.info(f"{sp_name} done")
            break
        except Exception as e:
            if 'deadlocked' in str(e) and i < MAX_RETRIES - 1:
                logging.info(f"Deadlock detected when executing {sp_name}. Retrying in {RETRY_DELAY} seconds.")
                time.sleep(RETRY_DELAY)
            else:
                logging.info(f"Failed to execute {sp_name} after {i + 1} attempts.")
                raise

if __name__ == '__main__':


    # Write the new data to the SQL table

    sqlinsert = text('''
        INSERT INTO stage.idm.drupal__user_profiles (
             user_id, adobe_id, subscriber_id, username, record_source,
             active, country, postcode, street_address, display_name,
             emails_primary, emails_type, email, resource_type, last_name,
             first_name, phone_type, contact_phone,
             timezone, user_consent, email_verified, gender,
             mobile_verified, date_of_birth, created_date, last_modified,
             verified_date, mobile_verified_date, year_of_birth,
             city_region,
             locality,
             record_load_dts_utc,
             hash_diff,
             newsletter_subs,
             marketing_id,
             marketing_id_email
             )
        SELECT
             user_id, adobe_id, subscriber_id, username, record_source,
             active, country, postcode, street_address, display_name,
             emails_primary, emails_type, email, resource_type, last_name,
             first_name, phone_type, contact_phone,
             timezone, user_consent, email_verified, gender,
             mobile_verified, date_of_birth, created_date, last_modified,
             verified_date, mobile_verified_date, year_of_birth,
             city_region,
             locality,
             record_load_dts_utc,
             hash_diff,
             newsletter_subs,
             marketing_id,
             marketing_id_email
        FROM stage.idm.temp_drupal__user_profiles
        WHERE user_id NOT IN (SELECT user_id FROM stage.idm.drupal__user_profiles);
    ''')

    sqlupdate = text('''
        UPDATE stage.idm.drupal__user_profiles
        SET
            adobe_id = s.adobe_id,
            subscriber_id = s.subscriber_id,
            username = s.username,
            record_source = s.record_source,
            active = s.active,
            country = s.country,
            postcode = s.postcode,
            street_address = s.street_address,
            display_name = s.display_name,
            emails_primary = s.emails_primary,
            emails_type = s.emails_type,
            email = s.email,
            resource_type = s.resource_type,
            last_name = s.last_name,
            first_name = s.first_name,
            phone_type = s.phone_type,
            contact_phone = s.contact_phone,
            timezone = s.timezone,
            user_consent = s.user_consent,
            email_verified = s.email_verified,
            gender = s.gender,
            mobile_verified = s.mobile_verified,
            date_of_birth = s.date_of_birth,
            created_date = s.created_date,
            last_modified = s.last_modified,
            verified_date = s.verified_date,
            mobile_verified_date = s.mobile_verified_date,
            year_of_birth = s.year_of_birth,
            city_region = s.city_region,
            locality = s.locality,
            record_load_dts_utc = s.record_load_dts_utc,
            hash_diff = s.hash_diff,
            newsletter_subs = s.newsletter_subs,
            marketing_id = s.marketing_id,
            marketing_id_email = s.marketing_id_email
        FROM stage.idm.temp_drupal__user_profiles s
        WHERE drupal__user_profiles.user_id = s.user_id;
    ''')

    # read data from api
    data = batch_get_users(API_URL,username,password,apiauth,COUNT,START_ID,END_ID,START_DATE,END_DATE,MODE)
    # # put data to a pandas dataframe
    data = read_list_of_dicts(data)
    # clean data
    data = replace_non_utf8(data)

    data = map_columns(data, UUID_NAMESPACE)
    data = remove_duplicates(data)
    # data = validate_data(data)  # Validate data using Pydantic

    # data to user_id, adobe_id, and active
    try:
        logging.info("writing data to cdw temp table temp_drupal__user_profiles")
        data.to_sql('temp_drupal__user_profiles',
                        engine,
                        schema=SCHEMA_NAME,
                        if_exists='replace',
                        index=False,
                        index_label='user_id',
                        # order the columns
                        # get the data types from the schema definition in SCHEMA_DRUPAL__USER_PROFILES
                        dtype={
                            'user_id': sqlalchemy.types.Integer(),
                            'adobe_id': sqlalchemy.types.String(255),
                            'active': sqlalchemy.types.String(255),
                            'country': sqlalchemy.types.String(255),
                            'postcode': sqlalchemy.types.String(255),
                            'street_address': sqlalchemy.types.String(255),
                            'display_name': sqlalchemy.types.String(255),
                            'emails_primary': sqlalchemy.types.String(255),
                            'emails_type': sqlalchemy.types.String(255),
                            'email': sqlalchemy.types.String(255),
                            'created_date': sqlalchemy.types.DateTime(timezone=True),
                            'last_modified': sqlalchemy.types.DateTime(timezone=True),
                            'resource_type': sqlalchemy.types.String(255),
                            'last_name': sqlalchemy.types.String(255),
                            'first_name': sqlalchemy.types.String(255),
                            'phone_type': sqlalchemy.types.String(255),
                            'contact_phone': sqlalchemy.types.String(255),
                            # 'roles': sqlalchemy.types.String(255),
                            # 'schemas': sqlalchemy.types.String(255),
                            'timezone': sqlalchemy.types.String(255),
                            'user_consent': sqlalchemy.types.String(255),
                            'date_of_birth': sqlalchemy.types.DateTime(),
                            'email_verified': sqlalchemy.types.String(255), # changed from Integer for csv
                            'verified_date': sqlalchemy.types.DateTime(timezone=True),
                            'gender': sqlalchemy.types.String(255),
                            'mobile_verified': sqlalchemy.types.String(255), # changed from Integer for csv
                            'mobile_verified_date': sqlalchemy.types.DateTime(timezone=True),
                            'subscriber_id': sqlalchemy.types.String(255),
                            'year_of_birth': sqlalchemy.types.DateTime(),
                            'username': sqlalchemy.types.String(255),
                            'record_source': sqlalchemy.types.String(255),
                            'city_region': sqlalchemy.types.String(255),
                            'locality': sqlalchemy.types.String(255),
                            'record_load_dts_utc': sqlalchemy.types.DateTime(),
                            'hash_diff': sqlalchemy.types.String(255),
                            'newsletter_subs': sqlalchemy.types.UnicodeText,
                            'marketing_id': sqlalchemy.types.String(36),
                            'marketing_id_email': sqlalchemy.types.String(36)
                            },
                            chunksize=1000
                        )
    except Exception as error:
        logging.error("Error with writing to temp table: " + str(error))

    # merge data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles
    try:
        # merge data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles
        with engine.connect() as conn:
            logging.info("inserting new data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles")
            result = conn.execute(sqlinsert)
            conn.commit()  # Committing the transaction
            logging.info(result.rowcount)
            logging.info("merging/updating data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles")
            result = conn.execute(sqlupdate)
            conn.commit()  # Committing the transaction
            logging.info(result.rowcount)

        # drop table idm.temp_drupal__user_profiles
        with engine.connect() as conn:
            logging.info("dropping temp table idm.temp_drupal__user_profiles")

            dropsql = text('DROP TABLE IF EXISTS stage.idm.temp_drupal__user_profiles;')  # Adjust the table name if needed
            result = conn.execute(dropsql)
            conn.commit()  # Committing the transaction
            logging.info("Temp table dropped successfully")

    except Exception as e:
        logging.error(e)
        logging.info('Error in merge')

    try:
        execute_sp_with_retry(engine, SCHEMA_NAME, sp_name="load_vault")

        # stored_procedure(engine,SCHEMA_NAME,SP_NAME="load_vault")

        execute_sp_with_retry(engine, SCHEMA_NAME, sp_name="load_braze_subs")
        # stored_procedure(engine,SCHEMA_NAME,SP_NAME="load_braze_subs")

        # select all user_id from the table and generate marketing_id if marketing_id is null using generate_uuid function

        # query
        external_id_null = '''
            SELECT user_id, email
            FROM idm.drupal__user_profiles
            WHERE marketing_id_email IS NULL;
            '''

        # execute query
        with engine.connect() as conn:
            external_id_null_df = pd.read_sql(external_id_null, conn)

        # generate marketing_id using generate_uuid function

        external_id_null_df['marketing_id_email'] = external_id_null_df['email'].apply(lambda x: create_uuid.generate_uuid(x, UUID_NAMESPACE))

        external_id_null_update = text('''
            UPDATE idm.drupal__user_profiles
            SET
                marketing_id_email = s.marketing_id_email
            FROM idm.temp_drupal__user_profiles_external_id s
            WHERE drupal__user_profiles.user_id = s.user_id
            AND drupal__user_profiles.marketing_id_email IS NULL;
            ''')

        external_id_null_df.to_sql('temp_drupal__user_profiles_external_id',
                        engine,
                        schema=SCHEMA_NAME,
                        if_exists='replace',
                        index=False,
                        index_label='user_id',
                        dtype={
                            'user_id': sqlalchemy.types.Integer(),
                            'marketing_id_email': sqlalchemy.types.String(36)
                            },
                            chunksize=50000
                        )
    except Exception as e:
        logging.error(e)
        logging.info('Error in sp or marketing id')

    try:
        # merge data into idm.drupal__user_profiles from idm.temp_drupal__user_profiles_external_id
        with engine.connect() as conn:
            conn.execute(external_id_null_update)
        # drop table
        with engine.connect() as conn:
            sqldropquery = text('DROP TABLE IF EXISTS idm.temp_drupal__user_profiles_external_id;')
            conn.execute(sqldropquery)
    except Exception as e:
        logging.error(e)
        logging.info('Error in merge')

  # # create the staging table
    # # drop table if exists idm.drupal__user_profiles;
    # with engine.connect() as conn:
    #     conn.execute('DROP TABLE IF EXISTS idm.drupal__user_profiles;')

    # # create table idm.drupal__user_profiles using idm.temp_drupal__user_profiles schema
    # with engine.connect() as conn:
    #     conn.execute(f'''CREATE TABLE idm.drupal__user_profiles (
    #         [user_id] [int] NULL,
    #         [adobe_id] [varchar](255) NULL,
    #         [subscriber_id] [varchar](255) NULL,
    #         [username] [varchar](255) NULL,
    #         [record_source] [varchar](255) NULL,
    #         [active] [varchar](255) NULL,
    #         [country] [varchar](255) NULL,
    #         [postcode] [varchar](255) NULL,
    #         [street_address] [varchar](255) NULL,
    #         [display_name] [varchar](255) NULL,
    #         [emails_primary] [varchar](255) NULL,
    #         [emails_type] [varchar](255) NULL,
    #         [email] [varchar](255) NULL,
    #         [resource_type] [varchar](255) NULL,
    #         [last_name] [varchar](255) NULL,
    #         [first_name] [varchar](255) NULL,
    #         [phone_type] [varchar](255) NULL,
    #         [contact_phone] [varchar](255) NULL,
    #         [timezone] [varchar](255) NULL,
    #         [user_consent] [varchar](255) NULL,
    #         [email_verified] [varchar](255) NULL,
    #         [gender] [varchar](255) NULL,
    #         [mobile_verified] [varchar](255) NULL,
    #         [date_of_birth] [datetime] NULL,
    #         [created_date] [datetimeoffset](7) NULL,
    #         [last_modified] [datetimeoffset](7) NULL,
    #         [verified_date] [datetimeoffset](7) NULL,
    #         [mobile_verified_date] [datetimeoffset](7) NULL,
    #         [year_of_birth] [datetime] NULL,
    #         [city_region] [varchar](255) NULL,
    #         [locality] [varchar](255) NULL,
    #         [record_load_dts_utc] [datetime] NULL,
    #         [hash_diff] [varchar](255) NULL
    #     )''')
