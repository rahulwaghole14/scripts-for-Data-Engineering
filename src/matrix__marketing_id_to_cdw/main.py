""" update marketing_id in stage.matrix.customer table in cdw"""
import logging
import os

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy.util import deprecations
from sqlalchemy.sql import text

from a_common.logging.logger import logger
from a_common.marketing.uuid_v5 import generate_uuid_v5

logger(
    "matrix_mar_id",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Load the environment variables from .env file
deprecations.SILENCE_UBER_WARNING = True
load_dotenv()


os.environ["SQLALCHEMY_SILENCE_UBER_WARNING"] = "1"  # Silence the message

UUID_NAMESPACE = os.environ.get("UUID_NAMESPACE")

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")
SCHEMA_NAME = "matrix"

# sql engine string
conn_str = (
    f"mssql+pyodbc://{username}:{password}@{server}/{database}"
    f"?driver={DRIVER}&TrustServerCertificate=yes"
)

# get list of hash_key and base_marketing_id
# from stage.matrix.customer table in cdw
QUERY = """
	with idm__users as (
		select user_id as hexa_account_id
			, subscriber_id as matrix__subscriber_subs_id
		from stage.idm.drupal__user_profiles
		where subscriber_id is not null
	)

	, matrix_customers as (
		select hash_key
			, subs_id
			, lower(trim(PersContact3)) as email
			, coalesce(MOBILE,HOME,WORK) as phone
		from stage.matrix.customer
		where marketing_id is NULL
		)

	, idm_matrix_merge as (
		select matrix_customers.hash_key
			, coalesce(cast(idm__users.hexa_account_id as nvarchar)
				, cast(matrix_customers.email as nvarchar)
				, cast(matrix_customers.phone as nvarchar)
				, null) as base_id
		from matrix_customers left join idm__users on idm__users.matrix__subscriber_subs_id = matrix_customers.subs_id
	)

	select * from idm_matrix_merge where base_id is not null
"""

QUERY_MERGE = """
	UPDATE stage.matrix.customer
	SET stage.matrix.customer.marketing_id = stage.matrix.temp_customer_marketing_id.marketing_id
	FROM stage.matrix.customer
	INNER JOIN stage.matrix.temp_customer_marketing_id
	ON stage.matrix.customer.hash_key = stage.matrix.temp_customer_marketing_id.hash_key
	WHERE stage.matrix.customer.marketing_id IS NULL
"""
# select data from stage.matrix.customer table in cdw with query and put into pandas dataframe


def run_update():
    """run update"""
    logging.info("running update")
    try:
        # create sql engine
        logging.info("creating sql engine")
        engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
        # get data from stage.matrix.customer table in cdw with query
        logging.info(
            "getting data from stage.matrix.customer table in cdw with query"
        )
        df = pd.read_sql_query(QUERY, engine)
        if df.empty:
            logging.info(
                "No records in dataframe, nothing to update in marketing id, finishing early today."
            )
        else:
            # create marketing id column
            logging.info("creating marketing id column")
            df["marketing_id"] = df["base_id"].apply(
                lambda x: generate_uuid_v5(x, UUID_NAMESPACE)
            )
            # drop base id
            df = df.drop("base_id", axis=1)
            # create temp tabl
            logging.info("Creating temp table to merge from")
            df.to_sql(
                "temp_customer_marketing_id",
                engine,
                schema=SCHEMA_NAME,
                if_exists="replace",
                index=False,
                dtype={
                    "hash_key": sqlalchemy.types.String(36),
                    "marketing_id": sqlalchemy.types.String(36),
                },
                chunksize=1000,
            )
            # run update from temp table to stage.matrix.customer table in cdw
            with engine.connect() as conn:
                logging.info(
                    "merging marketing ids to stage.matrix.customer table"
                )
                conn.execute(text(QUERY_MERGE))
                # drop temp table
                logging.info("dropping temp table")
                conn.execute(text(
                    "DROP TABLE stage.matrix.temp_customer_marketing_id;"
                ))
                # commit
                conn.commit()
                logging.info("done, good day")
    except Exception as error:  # pylint: disable=broad-except
        logging.error(error)


if __name__ == "__main__":

    run_update()
    logging.info("== END SCRIPT ==")
