import logging
import pandas as pd
from datetime import timedelta,datetime
from common.reports.report_query import process_report
from common.oberon_queries.queries.region_query import srp_region_query, old_region_report
from common.oberon_queries.oberon_functions import update_query_limit, update_query_datetime
from common.big_query.create_client import create_bq_client
from common.big_query.check_dataset_exists import check_bq_table
from common.big_query.upload_to_bq import biq_query_upload
from common.big_query.check_bq_dates import get_BQ_table_dates
from common.oberon_queries.queries.editorial_metrics_bq_schema import logged_in_users_bq_schema
from common.segments.segments_queries import segment_info, update_segment_defintion
from common.segments.update_segments import update_segment_defn, update_segment_negative_defn, fix_regions



def call_region_report(replatform=True):
    client = create_bq_client()
    dataset_id = 'hexa_replatform_analysis'
    table_id = 'region_report'
    logging.info(f'Running {table_id}.')
    if replatform:
        numdays = 7
        query = srp_region_query
        day_limit = 1
        query_time_period = 1
        region_segment = 's200000657_65b2d574176d3a4097d4b773'
        region = [3946610563,2532660862,3646184479,3176311920,4144815276,2157854076,4051110799,265552321,712305258,757256710,4225171186,485462375,493465973]

    else:
        numdays = 180
        query = old_region_report
        region_segment = 's200000657_65b2d574176d3a4097d4b773'
        region = [3946610563,2532660862,3646184479,3176311920,4144815276,2157854076,4051110799,265552321,712305258,757256710,4225171186,485462375,493465973]
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

    # Accidentally rewrote segment incorrectly, this was to fix.
    fix_segment = False
    if fix_segment:
        sys_env_segment = 's200000657_65b2d5dc7dd7b567b6c89567'
        sys_check = segment_info(sys_env_segment)
        #print(sys_check)

        new_reg = fix_regions(sys_check)
        print(new_reg)
        z = update_segment_defn(new_reg['definition'],region[0])
        print(z)
        update_segment_defintion(region_segment, z)
        region_def = segment_info(region_segment)
        print(region_def)
        quit()
    
    for date_ in dateList:
        date_start = date_.strftime('%Y-%m-%d')
        date_end = (date_ + timedelta(days = query_time_period)).strftime('%Y-%m-%d')

        if date_start not in bq_date_list:
            update_query_datetime(query, date_start, date_end)
            
            for reg in region + ['other']:
                region_def = segment_info(region_segment)
                if reg != 'other':
                    region_defn = update_segment_defn(region_def['definition'], reg)
                elif reg == 'other':
                    region_defn = update_segment_negative_defn(region_def['definition'], region)
                logging.info(f'Creating Query for {reg} - {date_start}')
                update_segment_defintion(region_segment, region_defn)
                df = process_report(query)
                if reg == 'other':
                    df['Region'] = 0
                else:
                    df['Region'] = reg
                df['x'] = pd.to_datetime(df['Day'], format='%b %d, %Y')
                df['date'] = df['x'].dt.strftime('%Y-%m-%d')
                df = df.drop(['x'], axis=1)
                if 'PVs (Sites) + AVs (Apps)' in df.columns:
                    df = df.rename(columns={'PVs (Sites) + AVs (Apps)': 'Page Views'})
                biq_query_upload(client,df, dataset_id, table_id, logged_in_users_bq_schema)


                                


