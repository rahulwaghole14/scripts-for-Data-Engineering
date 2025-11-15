import os
import logging
from logger import logger
import pandas as pd
from pydoc import describe
from datetime import timedelta,datetime
from common.reports.report_query import process_report
from common.oberon_queries.queries.editorial_metrics import srp_editorial_metrics_query, srp_editorial_metrics_query_no_referrer
from common.oberon_queries.oberon_functions import update_query_limit, update_query_datetime
from common.big_query.create_client import create_bq_client
from common.big_query.check_dataset_exists import check_bq_table
from common.big_query.upload_to_bq import biq_query_upload
from common.big_query.check_bq_dates import get_BQ_table_dates
from common.oberon_queries.queries.editorial_metrics_bq_schema import editoral_metrics_bq_schema

logger('replatform-analysis')

###############################################
##  Set number of days to run days to run    ##
###############################################

a = datetime.today()
numdays = 30
query_time_period = 1
dateList = []
for x in range (1, numdays):
    dateList.append(a - timedelta(days =  x))


##################################################
##  Set Query to Use from Oberon Debug Console  ##
##################################################
query = srp_editorial_metrics_query

############################
##  Set file name format. ##
############################
file_name_root = 'editorial_metrics'


update_query_limit(query, 10000)


###########################################
##  Set Big Query Dataset ID & Table ID  ##
###########################################

client = create_bq_client()
dataset_id = 'magic_metrics'
table_id = 'whetu-metrics-srp'




if __name__ == "__main__":
    if check_bq_table(client, dataset_id, table_id):
        bq_date_list = get_BQ_table_dates(client, dataset_id, table_id)
    else:
        bq_date_list = []

    logging.info('STARTED - Editorial Metrics')

    logging.info('Update Adobe Token')
    

    

    for date_ in dateList:
        date_start = date_.strftime('%Y-%m-%d')
        date_end = (date_ + timedelta(days = query_time_period)).strftime('%Y-%m-%d')


        logging.info(f'Creating Query for {date_start}')
        update_query_datetime(query, date_start, date_end)
        #if not(os.path.exists(data_file)):
        if date_start not in bq_date_list:
            try:
                logging.info('Downloading Report from Adobe Analytics')
                df = process_report(query)
                df['date'] = date_start
                biq_query_upload(client,df, dataset_id, table_id, editoral_metrics_bq_schema)
            except:
                logging.error(f'Bad data for {date_start}')


