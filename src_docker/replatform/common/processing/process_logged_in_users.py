
import pandas as pd
import logging
from common.big_query.create_client import create_bq_client
from common.oberon_queries.queries.editorial_metrics_bq_schema import logged_in_users_bq_schema, logged_in_user_detailed_bq_schema
from common.big_query.upload_to_bq import biq_query_upload_drop


def process_logged_in_users():
    client = create_bq_client()
    table_id = 'logged_in_user_counts'
    uv_table_id = 'logged_in_UVs'
    dataset_id = 'hexa_replatform_analysis'
    processed_table_id = 'logged_in_user_counts_processed'
    detailed_table_id = 'logged_in_users_processed'

    def get_BQ_profile_id_min_date(client, bq_dataset, bq_table):
        #output_list = []
        query = "SELECT \
            `Profile Id`, \
            MIN(`date`) AS `date`\
        FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "` GROUP BY `Profile Id`"
        
        df = client.query_and_wait(query).to_dataframe()

        return df

    def get_BQ_date_id_profile_id_count(client, bq_dataset, bq_table):
        #output_list = []
        query = "SELECT \
            `date`, \
            COUNT(`Profile Id`) AS `count_profile_id`\
        FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "` GROUP BY `date`"
        
        df = client.query_and_wait(query).to_dataframe()

        return df
    
    def get_BQ_date_id_logged_in_UVs(client, bq_dataset, bq_table):
        #output_list = []
        query = "SELECT `Unique Visitors`, date \
            FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "`"
        
        df = client.query_and_wait(query).to_dataframe()

        return df
    
    def get_BQ_replatform_users(client, bq_dataset, bq_table):
        #output_list = []
        query = "SELECT `Profile Id`, date \
            FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "`\
                  WHERE date >= '2024-01-17'"
        df = client.query_and_wait(query).to_dataframe()

        return df

    logging.info(f'Downloading {table_id} user min date.')
    min_dates = get_BQ_profile_id_min_date(client, dataset_id, table_id)
    logging.info(f'Downloading {table_id} user date count.')
    date_counts = get_BQ_date_id_profile_id_count(client, dataset_id, table_id)
    logging.info(f'Downloading {uv_table_id} table.')
    uv_counts = get_BQ_date_id_logged_in_UVs(client, dataset_id, uv_table_id)
    logging.info(f'Downloading {table_id} replatform only table.')
    df_replatform_user = get_BQ_replatform_users(client, dataset_id, table_id)


    logging.info(f'Joining data.')
    date_new = min_dates.groupby('date').agg({'Profile Id':'count'}).reset_index()
    df_join = pd.merge(date_new, date_counts,left_on='date',right_on='date')
    df_join.columns = ['date','New log ins','Total Logged in']
    df_join = pd.merge(df_join, uv_counts,left_on='date',right_on='date',how='left')
    df_join['Unique Visitors'] = df_join['Unique Visitors'].fillna(0)
    logging.info(f'Uploading data to  {processed_table_id}.')
    biq_query_upload_drop(client,df_join, dataset_id, processed_table_id, logged_in_users_bq_schema)
    df_replatform_new = pd.merge(df_replatform_user, min_dates, left_on = 'Profile Id', right_on='Profile Id')
    df_replatform_new.columns = ['Profile Id', 'date', 'min date']
    logging.info(f'Uploading data to  {detailed_table_id}.')
    biq_query_upload_drop(client,df_replatform_new, dataset_id, detailed_table_id, logged_in_user_detailed_bq_schema)
    