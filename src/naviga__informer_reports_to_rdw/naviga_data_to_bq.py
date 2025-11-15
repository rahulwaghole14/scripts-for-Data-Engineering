'''
For loading naviga data via API call directly to big query
'''
import os
import logging
import io
import csv
from datetime import datetime
from typing import get_origin, get_args, Union
import pandas as pd
import requests
from pydantic import ValidationError
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig
from dotenv import load_dotenv
from src.naviga__informer_reports_to_rdw.data_validator_base import naviga_data
from src.naviga__informer_reports_to_rdw.logger import logger

# Load environment variables
load_dotenv()
logger('naviga')

# Setup logger for logging info and errors
logging.basicConfig(level=logging.INFO)

# Define the API URL
api_url = os.environ.get("api_naviga")

def extract_base_type(type_hint):
    origin = get_origin(type_hint) or type_hint
    if origin is Union:
        # Handle Optional or other Unions; assumes the first type is the primary type
        return get_args(type_hint)[0]
    return origin

def pydantic_model_to_bq_schema(model):
    schema = []
    for field_name, field_type in model.__annotations__.items():
        base_type = extract_base_type(field_type)
        if base_type == int:
            bq_type = 'INTEGER'
        elif base_type == float:
            bq_type = 'FLOAT'
        elif base_type == bool:
            bq_type = 'BOOLEAN'
        elif base_type == str:
            bq_type = 'STRING'
        else:
            bq_type = 'STRING'  # Default type if not matched
        schema.append(SchemaField(field_name, bq_type))
    return schema

def adjust_dataframe_types(dataframe, model):
    for field_name, field_type in model.__annotations__.items():
        base_type = extract_base_type(field_type)
        if field_name in dataframe.columns:
            if base_type == int:
                dataframe[field_name] = pd.to_numeric(dataframe[field_name], errors='coerce').astype('Int64')
            elif base_type == float:
                dataframe[field_name] = pd.to_numeric(dataframe[field_name], errors='coerce')
            elif base_type == bool:
                dataframe[field_name] = dataframe[field_name].astype('bool')
            elif base_type == str:
                dataframe[field_name] = dataframe[field_name].astype('str')
            else:
                dataframe[field_name] = dataframe[field_name].astype('str')  # Convert other types to string by default

def fetch_and_validate_data(api_url):
    logging.info("Starting to connect to API URL")
    response = requests.get(api_url, timeout=500)
    if response.status_code == 200:
        logging.info("Data fetched successfully from API.")
        
        # Decode the content to a string
        content = response.content.decode('utf-8')
        
        # Use csv.reader to parse the CSV data
        reader = csv.DictReader(io.StringIO(content))
        
        # Convert to a list of dictionaries
        data_dicts = list(reader)
        
        validated_data = []
        for item in data_dicts:
            try:
                validated_model = naviga_data(**item)
                validated_data.append(validated_model.model_dump())  # Using model_dump() as per new Pydantic version
            except ValidationError as e:
                logging.error(f"Validation error: {e.json()}")
        
        # Convert validated data back to DataFrame
        return pd.DataFrame(validated_data)
    else:
        logging.error(f"Failed to fetch data: HTTP {response.status_code}")
        return None

def upload_data_to_bigquery(dataframe, dataset_id, table_id_base):
    client = bigquery.Client()
    # Format the table name with the current date
    current_date = datetime.now().strftime('%Y%m%d')
    table_id = f"{table_id_base}_bq_{current_date}" #adding bq to differ the new tables with the current ones
    table_full_path = f"{dataset_id}.{table_id}"
    schema = pydantic_model_to_bq_schema(naviga_data)  
    job_config = LoadJobConfig(schema=schema,write_disposition="WRITE_TRUNCATE")

    # Create a new table or overwrite an existing table
    job = client.load_table_from_dataframe(
        dataframe=dataframe, 
        destination=table_full_path,
        job_config=job_config
    )

    # Waits for the job to complete
    job.result()
    print(f"Data uploaded successfully to BigQuery table: {table_full_path}")

api_url = os.environ.get("api_naviga")  # URL to fetch data
dataset_id = os.environ.get("dataset_id_naviga")
table_id_base = os.environ.get("table_id_naviga")

# Fetch and validate data
dataframe = fetch_and_validate_data(api_url)
if dataframe is not None:
    logging.info("DataFrame fetched and validated successfully.")

    # Adjust types to match the BigQuery schema based on the Pydantic model
    adjust_dataframe_types(dataframe, naviga_data)

    # Generate schema from Pydantic model
    schema = pydantic_model_to_bq_schema(naviga_data)

    # Upload to BigQuery using the dynamically generated schema
    upload_data_to_bigquery(dataframe, dataset_id, table_id_base)
    logging.info("Data uploaded successfully to BigQuery.")
else:
    logging.error("No data fetched or validation failed; upload to BigQuery skipped.")
