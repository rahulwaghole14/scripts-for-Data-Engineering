'''
Author: Deepak Kumar Singh
Date: 30/03/2021
downloading records pagewise
'''

import requests
import json
import pandas as pd
import timeit

import logging
import warnings
import datetime
import os
from pathlib import Path
import numpy as np

from google.cloud import bigquery
from google.cloud.exceptions import NotFound  
from google.cloud import bigquery_storage #_v1beta1
from google.oauth2 import service_account

base_url = 'https://dashboard.presspatron.com'
headers = {'Authorization':'Token 20039f24-0dd8-4df8-8b02-92b1ef601b71'} # please add your token file

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
    handlers=[
        logging.FileHandler(os.getcwd()+"\\logs\\PressPatron_"+str(datetime.date.today())+".log"),
        logging.StreamHandler()
    ])

def download_dataframe_no_pagelimit(resource,df):

    flag = True
    page_number = 1
    print("For resource type, {}".format(resource))
    start = timeit.default_timer()
    while flag:
        try:
            url = base_url+'/api/v1/{0}?page={1}'.format(resource,page_number)
            res = requests.get(url, headers = headers)
            response_dict = json.loads(res.text)
            temp = pd.DataFrame(response_dict['items'])
            if temp.shape[0] == 0:
                flag = False
                break
            df = df.append(temp)
            page_number = page_number + 1
            
        except:
            flag = False

    stop = timeit.default_timer()

    df.reset_index(drop=True, inplace=True)
    logging.info('total execution time for "{0}" = {1} sec'.format(resource,stop-start))
    
    return df
    
def download_dataframe_with_pagelimit(resource,df, pagelimit):

    flag = True
    page_number = 1
    print("For resource type, {}".format(resource))
    start = timeit.default_timer()
    while flag and page_number <= pagelimit:
        try:
            url = base_url+'/api/v1/{0}?page={1}'.format(resource,page_number)
            res = requests.get(url, headers = headers)
            response_dict = json.loads(res.text)
            temp = pd.DataFrame(response_dict['items'])
            if temp.shape[0] == 0:
                flag = False
                break
            df = df.append(temp)
            page_number = page_number + 1
            
        except:
            flag = False

    stop = timeit.default_timer()

    df.reset_index(drop=True, inplace=True)
    logging.info('total execution time for "{0}" = {1} sec'.format(resource,stop-start))
    
    return df

def check_file_exist(filename, path):
    if os.path.isfile(path+'\\'+filename):
        return True
    else:
        return False

def append_dataframe_to_bigquery(df, client, table_id, bq_schema):

    if not df.empty:
        try:
            table = client.get_table(table_id)            
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            logging.info("Appending DataFrame with {} rows to {}".format(df.shape[0],table_id))
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() # Wait for the job to complete
        except NotFound:
            # Create a new table
            table = bigquery.Table(table_id, schema=bq_schema) 
            table = client.create_table(table)
            # Append dataframe to an empty table
            logging.info("Appending DataFrame with {} rows to new table: {}".format(df.shape[0],table_id))
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() # Wait for the job to complete
    else:
        logging.info("No dataframe to append")


def overwrite_dataframe_to_bigquery(df, client, table_id, bq_schema):

    if not df.empty:
        try:
            table = client.get_table(table_id)
            job_config = bigquery.LoadJobConfig(schema=bq_schema
                                                ,write_disposition="WRITE_TRUNCATE",)

            logging.info("Overwriting DataFrame with {} rows to {}".format(df.shape[0],table_id))
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() # Wait for the job to complete
        except NotFound:
            logging.info("Table '{}' is not found.".format(table_id))
            # Create a new table
            table = bigquery.Table(table_id, schema=bq_schema) 
            table = client.create_table(table)
            # Append dataframe to an empty table
            logging.info("Appending DataFrame with {} rows to new table: {}".format(df.shape[0],table_id))
            job_config = bigquery.LoadJobConfig(schema=bq_schema)
            job_config.allow_quoted_newlines = True
            job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() # Wait for the job to complete
    else:
        logging.info("No dataframe to append")

if __name__ == "__main__":

    ## parameters for big query
    path = os.getcwd()
    project_id = "hexa-data-report-etl-prod"
    # add path for your gcp security credential
    key_path = path + "\\secrets\\hexa-data-report-etl-prod-5b93b81b644e.json"
    credentials = service_account.Credentials.from_service_account_file(key_path,
                                                                        scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    
    ## big query schemas
    # schema for users
    schema_users = [
            bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("newsletterSubscribed", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("anonymous", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("firstName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("lastName", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("email", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("address", bigquery.enums.SqlTypeNames.STRING)            
        ]
    # schema for subscriptions
    schema_subscriptions = [
            bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("subscriptionId", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("cancellationAt", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("grossAmount", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("frequency", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("subscriptionStatus", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("rewardSelected", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("metadata", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("urlSource", bigquery.enums.SqlTypeNames.STRING)            
        ]
    # schema for transactions
    schema_transactions = [
            bigquery.SchemaField("createdAt", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("updatedAt", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("transactionId", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("grossAmount", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("cardIssueCountry", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("userId", bigquery.enums.SqlTypeNames.STRING), 
            bigquery.SchemaField("subscriptionId", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("frequency", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("processorCreditCardFee", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("netAmount", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("totalFees", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("processorBankTransferFee", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("pressPatronCommission", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("rewardSelected", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("metadata", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("urlSource", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("paymentStatus", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("pressPatronCommissionSalesTax", bigquery.enums.SqlTypeNames.STRING)
        ]
    
    # list of resources
    files = ["subscriptions","users","transactions"]
    # schema for each resource
    bq_schema = {'subscriptions':schema_subscriptions,'users':schema_users,'transactions':schema_transactions}
    # check if the table exists or not in bigquery
    for file in files:
        table_id = project_id+'.press_patron_api.'+file        
        try:
            table = client.get_table(table_id)
            logging.info("Table '{}' already exists and has {} rows.".format(table_id,table.num_rows))
            # getting the latest records pagewise
            df_new = pd.DataFrame()
            df_new = download_dataframe_no_pagelimit(file,df_new)
            ## reading existing records from bigquery
            query = "SELECT * FROM `"+table_id+"`"
            df_old = client.query(query).to_dataframe()
            # appending latest records with existing one
            df_new = df_new.append(df_old)
            df_new.reset_index(drop=True, inplace=True)
            # converting all columns to string as some column is of dict type
            all_columns = list(df_new)
            df_new[all_columns] = df_new[all_columns].fillna("").astype(str)
            # dropping duplicate records after appending
            df_new.drop_duplicates(inplace=True)
            df_new.reset_index(drop=True, inplace=True)
            overwrite_dataframe_to_bigquery(df_new, client, table_id, bq_schema[file])
        except NotFound:
            ## if not create new one
            logging.info("Table '{}' is not found.".format(table_id))
            # getting all records
            df = pd.DataFrame()
            df = download_dataframe_no_pagelimit(file,df)
            # converting all columns to string as some column is of dict type
            all_columns = list(df)
            df[all_columns] = df[all_columns].fillna("").astype(str)
            append_dataframe_to_bigquery(df, client, table_id, bq_schema[file])
            

    

    

    
    

    
    
