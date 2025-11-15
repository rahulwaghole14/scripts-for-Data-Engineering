''' piano subs '''
import hashlib
import logging
import os
import time
from datetime import datetime
import pytz

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import DDL, create_engine, inspect
from a_common.logging.logger import logger
from .create_uuid import generate_uuid

# Load the environment variables from .env file
logger('piano',
       os.path.join(os.path.dirname(os.path.realpath(__file__)), 'log'))
load_dotenv()

os.environ['SQLALCHEMY_SILENCE_UBER_WARNING'] = '1'  # Silence the message

UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")

SCHEMA = 'piano'
TABLE = 'piano__subscriptions'

endpoint = os.getenv('PIANO_ENDPOINT')
api_request = os.getenv('PIANO_API_REQUEST')
app_ids = os.getenv('PIANO_APP_IDS').split(',')
app_names = os.getenv('PIANO_APP_NAMES').split(',')
app_tokens = os.getenv('PIANO_APP_TOKENS').split(',')

# sql engine string
conn_str = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    f"?driver={DRIVER}&TrustServerCertificate=yes"
    )
REC_LIMIT = 200

# get unix timestamp for now minus 48 hours
# timestamp = int(time.time()) - (60 * 60 * 48)
# refreshing on mondays i.e. full weekend sat, sun, mon
timestamp = int(time.time()) - (60 * 60 * 24 * 7)

# get timestamp for last year start
# timestamp =  int(time.time()) - (60 * 60 * 24 * 365)

# convert to string
TIMESTAMP = str(timestamp)
logging.info('timestamp now minus 48 hours: %s', TIMESTAMP)


def fetch_all_data(nexturl, record_limit):
    ''' fetch all data in pages from api '''
    offset = 0
    datalist = []
    total_records = None

    while total_records is None or offset < total_records:
        fetched_data, limit, new_offset, total = query_endpoint(
            nexturl + f"&offset={offset}", record_limit
            )
        if fetched_data is None:
            break

        datalist.extend(fetched_data)
        offset = new_offset + limit
        total_records = total

    return datalist

def create_hash_string(row):
    ''' create hash string for record '''
    row_tuple = tuple(row.drop('record_load_dts'))
    sha256 = hashlib.sha256()
    sha256.update(repr(row_tuple).encode('utf-8'))
    return sha256.hexdigest()

# query endpoint with requests and get list of subs
def query_endpoint(input_url, limit ):
    ''' query endpoint '''

    input_url += '&limit=' + str(limit)

    try:
        response = requests.get(
            input_url
            ,timeout=300
            )
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON data from the 'Response' object
            json_data = response.json()
            # Extract the 'subscriptions' list from the JSON data
            subscriptions_data = json_data['subscriptions']
            limit = json_data['limit']
            offset = json_data['offset']
            total = json_data['total']
            count = json_data['count']
            logging.info('got response from piano')
            logging.info(
                'length of subscriptions_data: %s | limit: %s | offset: %s | total: %s | count: %s'
                , len(subscriptions_data)
                , limit
                , offset
                , total
                , count
                )
    except Exception as err: # pylint: disable=broad-except
        logging.error("error querying endpoint %s", err)
        subscriptions_data = None
        limit = None
        offset = None
        total = None
        count = None
    return subscriptions_data, limit, offset, total

# process
# get select_by create, update seperately
# get select_by create (firstly all time, then each 48 hours)
# merge into stage.piano.piano__subscriptions table in cdw
# get select_by update (firstly all time, then each 48 hours)
# merge into stage.piano.piano__subscriptions table in cdw

def convert_utc_to_nzt(unix_timestamp):
    """
    Convert a Unix timestamp to a datetime
    and convert to 'Pacific/Auckland' timezone.
    """

    # If it's not a number (NaN, None, etc.)
    if pd.isnull(unix_timestamp):
        return None

    # Convert the Unix timestamp to a datetime
    utc_time = datetime.utcfromtimestamp(unix_timestamp)
    utc_time = pytz.utc.localize(utc_time)

    # Convert to the 'Pacific/Auckland' timezone
    nzt = pytz.timezone('Pacific/Auckland')
    nzt_time = utc_time.astimezone(nzt)

    # Format the datetime as a string
    formatted_time = nzt_time.strftime('%Y-%m-%d %H:%M:%S')

    return formatted_time

def raw_data_to_dataframe(responsedata):
    ''' he wants a function '''

    response_df = pd.DataFrame(responsedata)
    for col in response_df.columns:
        if col not in ['user', 'term', 'resource']:
            #change the column name
            response_df.rename(columns={col: 'sub__' + col}, inplace=True)

    user_df = pd.json_normalize(response_df['user'])
    user_df.columns = ['user__' + col for col in user_df.columns]

    term_df = pd.json_normalize(response_df['term'])
    term_df.columns = ['term__' + col for col in term_df.columns]

    resourse_df = pd.json_normalize(response_df['resource'])
    resourse_df.columns = ['resource__' + col for col in resourse_df.columns]

    # Combine the original DataFrame with the new DataFrame(s)
    response_df = pd.concat([response_df.drop('user', axis=1), user_df], axis=1)
    response_df = pd.concat([response_df.drop('term', axis=1), term_df], axis=1)
    response_df = pd.concat([response_df.drop('resource', axis=1), resourse_df], axis=1)
    # replace dot '.' in column names with underscore '_'
    response_df.columns = response_df.columns.str.replace('.', '_', regex=False)

    # add column record source
    response_df['piano__app'] = app_name
    response_df['record_source'] = 'PIANO'
    # add column record_load_dts
    response_df['record_load_dts'] = datetime.now()

    # make name fields lowercase
    response_df['user__first_name'] = response_df['user__first_name'].str.lower()
    response_df['user__last_name'] = response_df['user__last_name'].str.lower()
    response_df['user__personal_name'] = response_df['user__personal_name'].str.lower()
    response_df['user__email'] = response_df['user__email'].str.lower()
    # trim string
    # resource.name
    response_df['term__resource_name'] = response_df['term__resource_name'].str.strip()

    #Add a column for the record change hash, excluding 'record_load_dts' column
    response_df['record_change_hash'] = response_df.apply(
        create_hash_string, axis=1
        )

    # convert to nzt:
    if 'sub__start_date' in response_df.columns:
        response_df['sub__start_date_nzt'] = response_df['sub__start_date'].apply(
            convert_utc_to_nzt)
    else:
        response_df['sub__start_date_nzt'] = None

    if 'sub__end_date' in response_df.columns:
        response_df['sub__end_date_nzt'] = response_df['sub__end_date'].apply(
            convert_utc_to_nzt)
    else:
        response_df['sub__end_date_nzt'] = None

    # obfuscate name columns (make them 'redacted'):
    response_df.loc[:, 'user__first_name'] = 'redacted'
    response_df.loc[:, 'user__last_name'] = 'redacted'
    response_df.loc[:, 'user__personal_name'] = 'redacted'
    response_df.loc[:, 'user__email'] = 'redacted'
    response_df.loc[:, 'sub__payment_method'] = 'redacted'
    response_df.loc[:, 'sub__shared_accounts'] = 'redacted'


    logging.info('raw data to dataframe')

    return response_df

def data_to_dataframe(responsedata, namespace): # pylint: disable=too-many-locals,too-many-statements,too-many-branches
    ''' Convert the list of dictionaries to a DataFrame '''
    response_df = pd.DataFrame(responsedata)
    # if column not 'user' or 'term' or 'resource', append 'sub__' to beginning
    for col in response_df.columns:
        if col not in ['user', 'term', 'resource']:
            #change the column name
            response_df.rename(columns={col: 'sub__' + col}, inplace=True)

    # check if user is present in dataframe
    if 'user' not in response_df.columns:
        try:
            logging.info('user column not found')
            # set user columns to None
            response_df['user'] = None
            # create dataframe for same number of rows in response_df
            # and set user columns to None
            # Create a new dataframe with the same number of rows as response_df
            user_df = pd.DataFrame(
                columns=['user__email', 'user__first_name', 'user__last_name',
                        'user__personal_name', 'user__uid', 'user__create_date'],
                        index=response_df.index
                        )

            user_df = user_df.applymap(lambda x: None)
        except Exception as err: # pylint: disable=broad-except
            logging.info('error: %s', err)

    else:
        user_df = pd.json_normalize(response_df['user'])
        user_df.columns = ['user__' + col for col in user_df.columns]

    if 'term' not in response_df.columns:
        try:
            logging.info('term column not found')
            # set user columns to None
            response_df['term'] = None

            term_df = pd.DataFrame(
                columns=[
                'term__term_id'
                ,'term__name'
                ,'term__update_date'
                ,'term__payment_currency'
                ,'term__currency_symbol'
                ,'term__payment_allow_promo_codes'
                ,'term__payment_first_price'
                ,'term__verify_on_renewal'
                ,'term__payment_allow_renew_days'
                ,'term__payment_new_customers_only'
                ,'term__payment_trial_new_customers_only'
                ,'term__payment_renew_grace_period'
                ,'term__payment_is_custom_price_available'
                ,'term__payment_is_subscription'
                ,'term__payment_has_free_trial'
                ,'term__payment_force_auto_renew'
                ,'term__payment_allow_gift'
                ,'term__is_allowed_to_change_schedule_period_in_past'
                ,'term__billing_config'
                ,'term__resource_rid'
                ,'term__resource_aid'
                ,'term__resource_deleted'
                ,'term__resource_disabled'
                ,'term__resource_create_date'
                ,'term__resource_update_date'
                ,'term__resource_publish_date'
                ,'term__resource_name'
                ,'term__resource_type'
                ,'term__resource_type_label'
                ,'term__resource_external_id'
                ,'term__resource_is_fbia_resource'
                ,'term__resource_bundle_type'
                ,'term__resource_bundle_type_label'],
                index=response_df.index
            )

            term_df = term_df.applymap(lambda x: None)
        except Exception as err: # pylint: disable=broad-except
            logging.info('error: %s', err)
    else:
        term_df = pd.json_normalize(response_df['term'])
        term_df = term_df.drop(columns=['create_date'])
        term_df.columns = ['term__' + col for col in term_df.columns]

    # Combine the original DataFrame with the new DataFrame(s)
    response_df = pd.concat([response_df.drop('user', axis=1), user_df], axis=1)
    response_df = pd.concat([response_df.drop('term', axis=1), term_df], axis=1)
    # replace dot '.' in column names with underscore '_'
    response_df.columns = response_df.columns.str.replace('.', '_', regex=False)

    columns_to_drop = [
    'term__change_options', 'resource', 'term__description',
    'term__payment_billing_plan_table', 'term__payment_billing_plan',
    'term__payment_billing_plan_description', 'sub__payment_method',
    'sub__billing_plan', 'sub__user_payment_info_id',
    'sub__psc_subscriber_number', 'sub__conversion_result',
    'sub__external_api_name', 'user__image1', 'user__last_visit',
    'term__type', 'term__type_name', 'term__shared_account_count',
    'term__shared_redemption_url', 'term__collect_address',
    'term__term_billing_descriptor', 'term__evt_verification_period',
    'term__evt', 'term__product_category', 'term__schedule',
    'term__schedule_billing', 'term__resource_description',
    'term__resource_image_url', 'term__resource_purchase_url',
    'term__resource_resource_url'
    ]

    # Find the intersection of columns in both the DataFrame and the list of columns to drop
    columns_to_drop = response_df.columns.intersection(columns_to_drop)

    # Drop the columns in the intersection
    response_df = response_df.drop(columns=columns_to_drop)

    # remove column if exist
    if 'term__schedule' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_periods'])
    if 'term__schedule_schedule_id' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_schedule_id'])
    if 'term__schedule_name' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_name'])
    if 'term__schedule_aid' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_aid'])
    if 'term__schedule_deleted' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_deleted'])
    if 'term__schedule_create_date' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_create_date'])
    if 'term__schedule_update_date' in response_df.columns:
        response_df = response_df.drop(columns=['term__schedule_update_date'])

    # add column record source
    response_df['piano__app'] = app_name
    response_df['record_source'] = 'PIANO'
    # add column record_load_dts
    response_df['record_load_dts'] = datetime.now()

    # make name fields lowercase
    response_df['user__first_name'] = response_df['user__first_name'].str.lower()
    response_df['user__last_name'] = response_df['user__last_name'].str.lower()
    response_df['user__personal_name'] = response_df['user__personal_name'].str.lower()
    response_df['user__email'] = response_df['user__email'].str.lower()
    # trim string
    # resource.name
    response_df['term__resource_name'] = response_df['term__resource_name'].str.strip()

    #Add a column for the record change hash, excluding 'record_load_dts' column
    response_df['record_change_hash'] = response_df.apply(
        create_hash_string, axis=1
        )

    # convert to nzt:
    if 'sub__start_date' in response_df.columns:
        response_df['sub__start_date_nzt'] = response_df['sub__start_date'].apply(
            convert_utc_to_nzt)
    else:
        response_df['sub__start_date_nzt'] = None

    if 'sub__end_date' in response_df.columns:
        response_df['sub__end_date_nzt'] = response_df['sub__end_date'].apply(
            convert_utc_to_nzt)
    else:
        response_df['sub__end_date_nzt'] = None

    columns_to_keep = [
       'sub__subscription_id'
      ,'sub__auto_renew'
      ,'sub__next_bill_date'
      ,'sub__upi_ext_customer_id'
      ,'sub__upi_ext_customer_id_label'
      ,'sub__cancelable'
      ,'sub__cancelable_and_refundadle'
      ,'sub__status'
      ,'sub__status_name'
      ,'sub__status_name_in_reports'
      ,'sub__start_date'
      ,'sub__start_date_nzt'
      ,'sub__end_date_nzt'
      ,'sub__is_in_trial'
      ,'sub__trial_period_end_date'
      ,'sub__trial_amount'
      ,'sub__trial_currency'
      ,'sub__charge_count'
      ,'user__first_name'
      ,'user__last_name'
      ,'user__personal_name'
      ,'user__email'
      ,'user__uid'
      ,'user__create_date'
      ,'term__aid'
      ,'term__term_id'
      ,'term__name'
      ,'term__update_date'
      ,'term__payment_currency'
      ,'term__currency_symbol'
      ,'term__payment_allow_promo_codes'
      ,'term__payment_first_price'
      ,'term__verify_on_renewal'
      ,'term__payment_allow_renew_days'
      ,'term__payment_new_customers_only'
      ,'term__payment_trial_new_customers_only'
      ,'term__payment_renew_grace_period'
      ,'term__payment_is_custom_price_available'
      ,'term__payment_is_subscription'
      ,'term__payment_has_free_trial'
      ,'term__payment_force_auto_renew'
      ,'term__payment_allow_gift'
      ,'term__is_allowed_to_change_schedule_period_in_past'
      ,'term__billing_config'
      ,'term__resource_rid'
      ,'term__resource_aid'
      ,'term__resource_deleted'
      ,'term__resource_disabled'
      ,'term__resource_create_date'
      ,'term__resource_update_date'
      ,'term__resource_publish_date'
      ,'term__resource_name'
      ,'term__resource_type'
      ,'term__resource_type_label'
      ,'term__resource_external_id'
      ,'term__resource_is_fbia_resource'
      ,'term__resource_bundle_type'
      ,'term__resource_bundle_type_label'
      ,'piano__app'
      ,'record_source'
      ,'record_load_dts'
      ,'record_change_hash'
      ,'sub__end_date'
    ]

    for column in response_df.columns:
        if column not in columns_to_keep:
            response_df.drop(column, axis=1, inplace=True)

    # generate uuid from user__uid
    response_df['marketing_id'] = response_df['user__uid'].apply(
        lambda x: generate_uuid(x, namespace))

    return response_df

def schema_exists(engine, schema):
    ''' check if schema exists '''
    inspector = inspect(engine)
    return schema in inspector.get_schema_names()

def create_schema(engine, schema):
    ''' create schema '''
    engine.execute(DDL(f"CREATE SCHEMA {schema}"))

def table_exists(engine, table_name, schema=None):
    ''' table exists '''
    inspector = inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)

# create sql-server table using to_sql
def merge_data_into_sql_table(new_data_frame, table_name, connection_str, schema=None):
    ''' create sql-server table merge data'''
    # Example usage
    # new_data_frame = pd.DataFrame(
    # {'key_column': [1, 2, 3]
    # , 'value_column': ['A', 'B', 'C']}
    # )  # Replace with your actual data
    # table_name = 'your_table_name'
    # merge_data_into_sql_table(new_data_frame, table_name, connection_str)

    try:
        # connect to sql server
        engine = create_engine(connection_str, fast_executemany=True)
        logging.info('connected to sql server')

            # Check if the schema exists and create it if it doesn't
        if schema and not schema_exists(engine, schema):
            create_schema(engine, schema)
            # Check if the table exists
        if not table_exists(engine, table_name, schema=schema):
            logging.info('table does not exist')
            # Create the table if it doesn't exist
            new_data_frame.to_sql(
                table_name
                , engine
                , if_exists='replace'
                , schema=schema
                , index=False
                )
            logging.info('created table in sql server')
        else:
            logging.info('table already exists')
            # Read the existing table into a DataFrame
            existing_data = pd.read_sql_table(table_name, engine,schema=schema)
            # length of existing_data:
            existing_data_length = len(existing_data)
            logging.info('length of existing data: %s', str(existing_data_length))
            logging.info('read existing data into a dataframe from cdw')

            # Merge the new data with the existing data
            key_column = 'sub__subscription_id'
            merged_data = pd.concat([existing_data, new_data_frame], ignore_index=True)
            merged_data = merged_data.drop_duplicates(subset=key_column, keep='last')
            merged_data_length = len(merged_data)
            logging.info('length of merged data: %s', str(merged_data_length))
            logging.info('merged data into a dataframe from cdw')

            # Write the merged data back to the SQL table
            # for col in merged_data.columns:
            #     for value in merged_data[col]:
            #         if isinstance(value, list):
            #             print(f"Found list in column: {col}, value: {value}")

            # if merged data greater than or equal to merge
            if merged_data_length >= existing_data_length:
                merged_data.to_sql(
                    table_name
                    , engine
                    , if_exists='replace'
                    , schema=schema
                    , index=False
                    )
                logging.info(
                    'merged data into a sql table from cdw'
                )
            else:
                logging.info('no new data to merge')
    except Exception as err: # pylint: disable=broad-except
        logging.error("error merging data into sql table %s", err)

for app_id, app_name, app_token in zip(app_ids, app_names, app_tokens):

    try:
        # Set the app_id, app_name, and app_token for the current iteration
        url = endpoint + api_request
        url += '?aid=' + app_id
        url += '&api_token=' + app_token
        url += '&start_date=' + TIMESTAMP
        url += '&select_by=create'  # Fetch data based on created date

        # Call the functions to process the data for the current app_id, app_name, and app_token
        data = fetch_all_data(url, REC_LIMIT)
        data_frame = data_to_dataframe(data, UUID_NAMESPACE)

        merge_data_into_sql_table(data_frame, TABLE, conn_str, SCHEMA)

        # Set the app_id, app_name, and app_token for the current iteration
        url = endpoint + api_request
        url += '?aid=' + app_id
        url += '&api_token=' + app_token
        url += '&start_date=' + TIMESTAMP
        url += '&select_by=update'  # Fetch data based on update date

        # Call the functions to process the data for the current app_id, app_name, and app_token
        data = fetch_all_data(url, REC_LIMIT)
        data_frame = data_to_dataframe(data, UUID_NAMESPACE)
        merge_data_into_sql_table(data_frame, TABLE, conn_str, SCHEMA)
    except Exception as error: # pylint: disable=broad-except
        logging.info('error: %s', error)
