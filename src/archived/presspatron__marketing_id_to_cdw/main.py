''' do something '''
# pylint: disable-all

import logging
import os

import pandas as pd
import sqlalchemy
from create_uuid import generate_uuid
from dotenv import load_dotenv
from sqlalchemy.util import deprecations
from sqlalchemy import inspect

# Load the environment variables from .env file
deprecations.SILENCE_UBER_WARNING = True
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
SCHEMA_NAME = 'presspatron'
TABLE_NAME = 'braze_user_profiles'

# sql engine string
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={DRIVER}&TrustServerCertificate=yes"

def table_exists(engine, table_name, schema=None):
    ''' table exists '''
    inspector = inspect(engine)
    return table_name in inspector.get_table_names(schema=schema)

# get list of hash_key and base_id from stage.presspatron.supporter table in cdw
# 	-- we need to create new table from this data with new marketing_id column
# -- stage.presspatron.supporter
query = '''
with existing as (
    select hash_key, marketing_id from stage.presspatron.braze_user_profiles
)

, source as (
    select s.*
        , lower(trim(s.[Email address])) as base_id
        from stage.presspatron.supporter s
    -- where s.[Subscribed to newsletter?] = 'Yes'
)

select source.*
from source
left join existing on source.hash_key = existing.hash_key
where existing.marketing_id is null;
'''

query_insert = '''
INSERT INTO stage.presspatron.braze_user_profiles (
    record_load_dts
    , sequence_nr
    , TransactionID
    , [Sign up date]
    , [First name]
    , [Last name]
    , [Email address]
    , Currency
    , [Subscribed to newsletter?]
    , [Active supporter?]
    , [Frequency of recurring payment]
    , [Recurring contribution amount?]
    , [Total contributions to date]
    , record_source
    , hash_key
    , hash_diff
    , marketing_id
)
SELECT
    record_load_dts
    , sequence_nr
    , TransactionID
    , [Sign up date]
    , [First name]
    , [Last name]
    , [Email address]
    , Currency
    , [Subscribed to newsletter?]
    , [Active supporter?]
    , [Frequency of recurring payment]
    , [Recurring contribution amount?]
    , [Total contributions to date]
    , record_source
    , hash_key
    , hash_diff
    , marketing_id
FROM stage.presspatron.temp_braze_user_profiles temp
WHERE NOT EXISTS (
    SELECT 1
    FROM stage.presspatron.braze_user_profiles main
    WHERE main.hash_key = temp.hash_key
);
'''

query_merge = '''
	UPDATE stage.presspatron.braze_user_profiles
	SET stage.presspatron.braze_user_profiles.marketing_id = stage.presspatron.temp_braze_user_profiles.marketing_id
	FROM stage.presspatron.braze_user_profiles
	INNER JOIN stage.presspatron.temp_braze_user_profiles
	ON stage.presspatron.braze_user_profiles.hash_key = stage.presspatron.temp_braze_user_profiles.hash_key
	WHERE stage.presspatron.braze_user_profiles.marketing_id IS NULL;
'''
# select data from stage.matrix.customer table in cdw with query and put into pandas dataframe

try:
	# create sql engine
	logging.info('creating sql engine')
	engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
	# get data from stage.matrix.customer table in cdw with query
	logging.info('getting data from stage.presspatron.supporter table in cdw with query')
	df = pd.read_sql_query(query, engine)
	logging.info(df.shape)
	if df.empty:
		logging.info('No records in dataframe, nothing to update in marketing id, finishing early today.')
	else:
		# create marketing id column
		logging.info('creating marketing id column')
		df['marketing_id'] = df['base_id'].apply(lambda x: generate_uuid(x, UUID_NAMESPACE))
		# drop base id
		df = df.drop('base_id', axis=1)
		# create user_profile table
		logging.info('Creating temp table to merge from')
		df.to_sql('temp_braze_user_profiles',
							engine,
							schema=SCHEMA_NAME,
							if_exists='replace',
							index=False,
							chunksize=1000
							)
		if not table_exists(engine, TABLE_NAME, SCHEMA_NAME):
			logging.info('table does not exist')
			# Create the table if it doesn't exist
			df.to_sql(
				TABLE_NAME
				, engine
				, if_exists='replace'
				, schema=SCHEMA_NAME
				, index=False
				, chunksize=1000
				)
		else:
			with engine.connect() as conn:
				logging.info('inserting new records to braze_user_profiles table')
				conn.execute(query_insert)
				logging.info('merging marketing ids to stage.presspatron.braze_user_profiles table')
				conn.execute(query_merge)
				# drop temp table
				logging.info('dropping temp table')
				conn.execute('DROP TABLE stage.presspatron.temp_braze_user_profiles;')
				logging.info('done, good day')
except Exception as error:
	logging.error(error)
