from common.oberon_queries.queries.flow_queries import *
from common.reports.report_query import process_report
from common.big_query.create_client import create_bq_client
from common.oberon_queries.oberon_functions import update_query_datetime, update_query_segment

import logging
import pandas as pd
from datetime import timedelta, datetime
from common.big_query.upload_to_bq import biq_query_upload
from common.big_query.check_dataset_exists import check_bq_table
from common.oberon_queries.queries.flow_schema import flow_bq_schema
from common.big_query.check_bq_dates import get_BQ_table_dates

'''
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler("adobe_analytics_2.log"),
        logging.StreamHandler()
    ])

replatform = False
'''


sys_env_mapping = {
    's200000657_65f3766facc2414a5068dcb2':'iOS',
    's200000657_65f37661acc2414a5068dcb1':'Android',
    's200000657_65f3764d6ef034233393eab2':'web'
}





###########################################
##  Set Big Query Dataset ID & Table ID  ##
###########################################







def call_flow_report(replatform=True):

    if replatform:
        date_offset = 0
        sys_envs = ['s200000657_65f3766facc2414a5068dcb2','s200000657_65f37661acc2414a5068dcb1','s200000657_65f3764d6ef034233393eab2']
        table_id = 'flow-analysis-srp-new'
        entry_variable = "variables/evar3"
        entry_page_query = [entry_page_q]
        flow_query = q_custom
    else:
        date_offset = 75
        sys_envs = ['s200000657_65f38c796ef034233393ead8','s200000657_65f38caaacc2414a5068dce2','s200000657_660361a235ef036a03d891b0']
        table_id = 'flow-analysis-old-new'
        entry_variable = "variables/evar94"
        entry_page_query = [entry_page_q_old]
        flow_query = q_custom_old

    a = datetime.today()
    numdays = 30
    query_time_period = 1
    dateList = []
    for x in range (1, numdays):
        dateList.append(a - timedelta(days = (date_offset + x)))
    #dateList.reverse()
    
    client = create_bq_client()
    dataset_id = 'hexa_data_flow_analysis'


    if check_bq_table(client, dataset_id, table_id):
        bq_date_list = get_BQ_table_dates(client, dataset_id, table_id)
    else:
        bq_date_list = []



    

    def page_type_string(page_type_list):
        output = []
        for page_type in page_type_list:
            output.extend(
            [
                {
                    "func": "container",
                    "context": "hits",
                    "pred": {
                        "func": "streq",
                        "str": page_type,
                        "val": {
                            "func": "attr",
                            "name": entry_variable,
                            "allocation-model": {
                                "func": "allocation-instance"
                            }
                        },
                        "description": "Page Type"
                    }
                },
                {
                    "func": "dimension-restriction",
                    "count": 1,
                    "limit": "within",
                    "attribute": {
                        "func": "attr",
                        "name": entry_variable,
                        "allocation-model": {
                            "func": "allocation-instance"
                        }
                    }
                }
            ]
            )
        return output


    def run_report_daily(date_):
        date_start = date_.strftime('%Y-%m-%d')
        date_end = (date_ + timedelta(days = query_time_period)).strftime('%Y-%m-%d')
        if date_start not in bq_date_list:
            for segment in sys_envs:
                
                logging.info(f'Creating Query for {date_start}')
                # generate initial list and entries

                for x in entry_page_query:
                    update_query_datetime(x, date_start, date_end)
                    update_query_segment(x,segment)
                    df_entries = process_report(x)

                flow_list = []

                df_entries.columns = ['Page Type','Visits']
                df_entries['Entry'] = 'Entry'
                df_entries['Date']  = date_start
                df_entries['sys_env']  = segment
                #df_entries.to_csv(f'Entries_{date_start}.csv')
                
                biq_query_upload(client,df_entries, dataset_id, table_id, flow_bq_schema)
                for index, row in df_entries.iterrows():
                    flow_list.append([row['Page Type']])
                #print(flow_list)

                df_flows = pd.DataFrame()

                for flow in flow_list:
                    #print(len(flow_list))
                    #print(flow)
                    flow_json_list = page_type_string(flow)
                    query = flow_query(flow_json_list, segment)
                    update_query_datetime(query, date_start, date_end)
                    df_f = process_report(query,skip_rename=True,rename='Page Type')
                    df_f['Entry'] = pd.Series([flow.copy() for x in range(len(df_f.index))])
                    for index, row in df_f.iterrows():
                        if row['Visits'] > 5000:
                            new_flow = flow + [row['Page Type']]
                            flow_list.append(new_flow)
                    #df_flows = df_flows.append(df_f, ignore_index=True)
                    #print(max(df_f['Visits']))
                    df_flows = pd.concat([df_flows,df_f], ignore_index=True)
                    #print(df_flows)
                df_flows['Date'] = date_start
                df_flows['sys_env'] = segment
                #print(df_flows)
                biq_query_upload(client,df_flows, dataset_id, table_id, flow_bq_schema)
                #df_flows.to_csv(f'Flows_{date_start}.csv')


    i = 0
    for d in dateList:
        logging.info(f'Running Date {str(d)}, {i} of {len(dateList)}')
        run_report_daily(d)
        i = i + 1
