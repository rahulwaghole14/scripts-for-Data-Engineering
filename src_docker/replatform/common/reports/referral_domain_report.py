import logging
from datetime import timedelta,datetime
from common.reports.report_query import process_report
from common.oberon_queries.queries.referral_domain import srp_referral_domain, old_referral_domain
from common.oberon_queries.oberon_functions import update_query_limit, update_query_datetime
from common.big_query.create_client import create_bq_client
from common.big_query.check_dataset_exists import check_bq_table
from common.big_query.upload_to_bq import biq_query_upload
from common.big_query.check_bq_dates import get_BQ_table_dates
from common.oberon_queries.queries.editorial_metrics_bq_schema import logged_in_users_bq_schema




def call_referral_domain_report(replatform=True):
    client = create_bq_client()
    dataset_id = 'hexa_replatform_analysis'
    table_id = 'referral_domain'
    logging.info(f'Running {table_id}.')
    if replatform:
        numdays = 8
        query = srp_referral_domain
        day_limit = 1
        query_time_period = 1

    else:
        numdays = 180
        query = old_referral_domain
        day_limit = 1
        query_time_period = 1

    a = datetime.today()
    dateList = []
    for x in range (day_limit, numdays):
        dateList.append(a - timedelta(days =  x))

    update_query_limit(query, 10000)

    if check_bq_table(client, dataset_id, table_id):
        bq_date_list = get_BQ_table_dates(client, dataset_id, table_id)
    else:
        bq_date_list = []

    

    for date_ in dateList:
        date_start = date_.strftime('%Y-%m-%d')
        date_end = (date_ + timedelta(days = query_time_period)).strftime('%Y-%m-%d')

        if date_start not in bq_date_list:
            update_query_datetime(query, date_start, date_end)
            logging.info(f'Downloading {table_id} from Adobe Analytics for {date_start}')
            df = process_report(query)
            df['date'] = date_start
            df.columns = ['Referring Domain',
                            'Page Views - Domestic Traffic NZ',
                            'Page Views - Desktop Site',
                            'Page Views - Mobile Site',
                            'Page Views - Android App',
                            'Page Views - iOS App',
                            'Unique Visitors - Domestic Traffic NZ',
                            'Unique Visitors - Desktop Site',
                            'Unique Visitors - Mobile Site',
                            'Unique Visitors - Android App',
                            'Unique Visitors - iOS App',
                            'Visits - Domestic Traffic NZ',
                            'Visits - Desktop Site',
                            'Visits - Mobile Site',
                            'Visits - Android App',
                            'Visits - iOS App',
                            'date']
            biq_query_upload(client,df, dataset_id, table_id, logged_in_users_bq_schema)
