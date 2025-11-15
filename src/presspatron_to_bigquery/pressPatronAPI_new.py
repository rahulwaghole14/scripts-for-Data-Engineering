'''
Author: Deepak Kumar Singh
Date: 30/03/2021
downloading records pagewise
Update_Date: 09/01/2024
Update_By: Amber Wang
Description: to re-create the tables in au location
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
import gc

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
            df = pd.concat([df, temp], ignore_index=True)
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
            df = pd.concat([df, temp], ignore_index=True)
            page_number = page_number + 1
            
        except:
            flag = False

    stop = timeit.default_timer()

    df.reset_index(drop=True, inplace=True)
    logging.info('Total download time for "{0}" = {1} sec'.format(resource,stop-start))
    
    return df

def check_file_exist(filename, path):
    if os.path.isfile(path+'\\'+filename):
        return True
    else:
        return False

def ensure_dataset_in_australia(client, dataset_id):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        dataset = client.get_dataset(dataset_ref)
        if dataset.location != "australia-southeast1":
            raise Exception("Dataset found but in the wrong location.")
        logging.info(f"Dataset '{dataset_id}' exists in Australia.")
    except NotFound:
        logging.info(f"Dataset '{dataset_id}' not found. Creating in Australia.")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "australia-southeast1"
        client.create_dataset(dataset, exists_ok=True)
    except Exception as e:
        logging.error(f"Error with dataset: {e}")

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
        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
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
    dataset_id = 'press_patron_api_data'
    # add path for your gcp security credential
    key_path = path + "\\secrets\\hexa-data-report-etl-prod-5b93b81b644e.json"
    credentials = service_account.Credentials.from_service_account_file(key_path,
                                                                        scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    ensure_dataset_in_australia(client, dataset_id)
    
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
            bigquery.SchemaField("address", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING)
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
            bigquery.SchemaField("urlSource", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING)
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
            bigquery.SchemaField("pressPatronCommissionSalesTax", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("load_dts", bigquery.enums.SqlTypeNames.STRING)
        ]
    
    # list of resources
##    files = ["subscriptions","users","transactions"]
##    files = ["users"] #Testing api problem on 2022Aug10
    files = ["subscriptions","transactions", "users"]

    # schema for each resource
    bq_schema = {'subscriptions':schema_subscriptions,'users':schema_users,'transactions':schema_transactions}
    # check if the table exists or not in bigquery
for file in files:
    table_id = project_id+'.'+dataset_id+'.'+file        
    try:
        table = client.get_table(table_id)
        logging.info("Table '{}' already exists and has {} rows.".format(table_id,table.num_rows))
        # getting the latest records pagewise
        df_new = pd.DataFrame()
        df_new = download_dataframe_with_pagelimit(file,df_new,200)
        all_columns = list(df_new)
        df_new[all_columns] = df_new[all_columns].fillna("").astype(str)
        print(df_new.info())

        # Check if the table is empty
        if table.num_rows == 0:
            logging.info("Table '{}' is empty. Appending data.".format(table_id))
            df_new['load_dts'] = str(pd.Timestamp.now())
            append_dataframe_to_bigquery(df_new, client, table_id, bq_schema[file])
        else:
            # reading existing records from bigquery
            query = "SELECT * FROM `"+table_id+"`"
            df_old = client.query(query).to_dataframe()
            if not df_old.empty:
                # dropping the load_dts column
                if 'load_dts' in df_old.columns:
                    df_old.drop(columns='load_dts', inplace=True)
                else:
                    print("Column 'load_dts' does not exist in the DataFrame.")
                print(df_old.info())
                df_all = df_new.merge(df_old.drop_duplicates(), on = all_columns, how='left', indicator=True)
                del df_old
                gc.collect()
                mask = df_all['_merge'] == 'left_only'
                df_new = df_all.loc[mask]
                df_new.drop(columns='_merge', inplace=True)
                print(df_new.info())
                df_new['load_dts'] = str(pd.Timestamp.now())
                print(df_new.info())
                append_dataframe_to_bigquery(df_new, client, table_id, bq_schema[file])

    except NotFound:
        # If table is not found, create new one and append data
        logging.info("Table '{}' is not found. Creating new table.".format(table_id))
        df = pd.DataFrame()
        df = download_dataframe_no_pagelimit(file,df)
        all_columns = list(df)
        df['load_dts'] = str(pd.Timestamp.now())
        df[all_columns] = df[all_columns].fillna("").astype(str)
        append_dataframe_to_bigquery(df, client, table_id, bq_schema[file])  
