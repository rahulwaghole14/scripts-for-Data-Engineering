''' main app '''
# pylint: disable=all

import logging
import os
import time

import pandas as pd
import sqlalchemy
from api_composer.clean_articles import add_columns
from api_composer.get_articles import (
    get_articles_list,
    get_articles_to_dataframe,
    init_session
    )
from dotenv import load_dotenv
from sqlserver_cdw.load_articles import create_table, load_data, merge_data
# from sqlserver_cdw.run_stored_procedure import stored_procedure

# logger('composer')
# Load the environment variables from .env file
load_dotenv()

PAGELIMIT = 500

# normal behavior:
day_before_yesterday = (pd.Timestamp.today() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
first_day = day_before_yesterday

# batch update history:
# between two dates:
# first_day = '2023-05-01'
# last_day = '2023-05-31'

TABLE_NAME = "composer__articles"
SCHEMA_NAME = "composer"

# nzcms-production-varnish-internal.fairfaxmedia.co.nz/services/content/v1/article/300992767&q=(firstPublishTime%3Dge%3D2023-09-10T00%3A00%3A00%3B
# &q=(firstPublishTime%3Dge%3D2023-09-10T00%3A00%3A00%3B)

# SQLServer endpoint and credentials
server = os.environ.get("DB_SERVER")
port = os.environ.get("DB_PORT")
database = os.environ.get("DB_DATABASE")
username = os.environ.get("DB_USERNAME")
password = os.environ.get("DB_PASSWORD")
DRIVER = os.environ.get("DB_DRIVER")

# api endpoint details
COMPOSER_API_URL = os.environ.get("COMPOSER_API_URL")
url = (
    f"{COMPOSER_API_URL}"
    f"?direction=asc&orderby=assetId&limit={PAGELIMIT}"
    f"&q=(firstPublishTime%3Dge%3D{first_day}T00%3A00%3A00%3B"
    # f"firstPublishTime%3Dle%3D{last_day}T23%3A59%3A59.999%3B" # between two dates
    f"status%3Dmatch%3DREADY%3BlastActionPerformed%3Dmatch%3DPUBLISH)"
    )

# Set up the database connection
conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={DRIVER}&TrustServerCertificate=yes"
engine = sqlalchemy.create_engine(conn_str, fast_executemany=True)
conn_str_smart = f"mssql+pyodbc://{username}:{password}@{server}/smart?driver={DRIVER}&TrustServerCertificate=yes"
engine_smart = sqlalchemy.create_engine(conn_str_smart, fast_executemany=True)

if __name__ == '__main__':

    ## start timing
    start = time.time()
    session = init_session()

    ## get a list of article ids
    # data = [85811271,300892591,128296235,77536927,300942622,300970485]
    # data = [300974727,132984136,300974895,300975757,300957563,300960302,132938576,300960038,132988766,300975858,300974736,300957454,132977830,132981534,300970835,132974462,300970772,300970755,132851085,300953363,300976175,300970742,132980555,300968970,300974648,300966742,300955341,300953566,300968943,132937202,132972721,300957447,300965689,300970485,300961269,300975328,300974730,300975663,300975264,132972613,300966754,300975571,300976265,132876474,300967422,300957435,300953275,300970789,132985834,132982963,300971775,300974712,132886937,132934109,300975819,300974163,300954020,132888177,300955573,300975105,300975763,300975158,132972277,300958478,300966741,300970279,300973712,132970792,132990872,300976251,300974196,132940187,132975687,132951139,300953558,132971089,300976050,132984395,300975191,132968868,132992031,300953896,132969414,300966378,300974742,132981505,300940066,132964563,300966743,300969583,132966400,132934433,300973947,300973968,300974142,300954463,300958212,132981014,300973571,132957664,300957449,300969140,300970792,132874215,300942622,132876766]
    # data = [300970485,300942622]
    data = get_articles_list(url, session)

    # print("articles to get between dates: " + first_day + " and " + last_day + " are: " + str(len(data))) # between two dates
    logging.info("articles to get between dates: %s and NOW are: %s", first_day, str(len(data)))

    ## create dataframe
    data_frame = get_articles_to_dataframe(data, COMPOSER_API_URL, session)
    session.close()
    data_frame = add_columns(data_frame)

    ## create table in cdw (only if it doesn't exist already)
    # create_table(TABLE_NAME, SCHEMA_NAME, engine)

    ## load, merge data into cdw temp_ table and merge from temp_ to cdw table in stage db
    load_data(data_frame, TABLE_NAME, SCHEMA_NAME, engine)
    merge_data(TABLE_NAME, SCHEMA_NAME, engine)

    ## stop timer
    end = time.time()
    logging.info("total time taken: %s", str(end - start))
