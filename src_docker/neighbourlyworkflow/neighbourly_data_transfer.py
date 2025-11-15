import os
import sys
import pymysql
import pandas as pd
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, NotFound
from typing import List, Tuple
import logging
import json
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import paramiko
import socket
import threading
import io
import base64
import paramiko

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from neighbourlyworkflow.validation import create_dynamic_table_model, validate_data
from common.bigquery.bigquery import create_bigquery_client, create_table_if_not_exists
from common.aws.aws_secret import get_secret
from common.logging.logger import logger, log_start, log_end

# Load environment variables
load_dotenv()

# Configure logger
logger = logger(
    "neighbourly_data_transfer",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

try:
    google_credsbase64_json = get_secret("datateam_google_cred_prod_base64")
    base64_credentials = json.loads(google_credsbase64_json)
    base64_credentials = base64_credentials['GOOGLE_CLOUD_CRED_BASE64']

    neighbourly_secrets_json = get_secret("neighbourly_secrets")
    neighbourly_private_key = get_secret("nbly_ssh_aw")
    neighbourly_secrets = json.loads(neighbourly_secrets_json)
except json.JSONDecodeError as e:
    logger.error(f"Failed to parse JSON from secrets: {e}")
    sys.exit(1)


# SSH jump host details
jump_host = neighbourly_secrets['nbly_jump_host']
jump_port = int(neighbourly_secrets['nbly_jump_port'])
jump_username = neighbourly_secrets['nbly_jump_user']

# MySQL server details
mysql_host = neighbourly_secrets['nbly_mysql_host']
mysql_port = int(neighbourly_secrets['nbly_mysql_port'])
mysql_user = neighbourly_secrets['nbly_mysql_user']
mysql_password = neighbourly_secrets['nbly_mysql_user_pw']
mysql_db = neighbourly_secrets['nbly_db_name']

# Define the list of tables to be transferred
tables_to_transfer = [
    'notification_preference',
    'paf_address',
    'user_paf_address',
    'user',
    'neighbourhood'
]

def get_mysql_table_schema(cursor, table_name):
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = '{mysql_db}' AND TABLE_NAME = '{table_name}';
    """
    cursor.execute(query)
    return cursor.fetchall()

def mysql_to_bigquery_schema(table_name: str, schema_info: List[Tuple[str, str]]) -> List[bigquery.SchemaField]:
    # logger.info(f"Creating BigQuery schema for table: {table_name}")
    schema = []

    for column_name, column_type in schema_info:
        logger.debug(f"Processing column: {column_name}, MySQL type: {column_type}")
        column_type = column_type.lower()
        if column_name == 'center_point':
            bq_type = "STRING"  # We'll store the base64 encoded string,decoded_center_point = base64.b64decode(encoded_center_point)
        elif column_type in ['int', 'smallint', 'mediumint', 'bigint']:
            bq_type = "INTEGER"
        elif column_type.startswith('tinyint(1)'):
            bq_type = "BOOLEAN"
        elif column_type.startswith('tinyint'):
            bq_type = "INTEGER"
        elif column_type in ['float', 'double', 'decimal']:
            bq_type = "FLOAT"
        elif column_type in ['varchar', 'char', 'text', 'longtext']:
            bq_type = "STRING"
        elif column_type == 'date':
            bq_type = "DATE"
        elif column_type in ['datetime', 'timestamp']:
            bq_type = "TIMESTAMP"
        elif column_type == 'time':
            bq_type = "TIME"
        elif column_type in ['blob', 'binary', 'varbinary']:
            bq_type = "BYTES"
        else:
            logger.warning(f"Unknown MySQL type: {column_type} for column {column_name}. Defaulting to STRING.")
            bq_type = "STRING"

        logger.debug(f"Mapped {column_name} to BigQuery type: {bq_type}")
        schema.append(bigquery.SchemaField(column_name, bq_type, mode="NULLABLE"))

    # logger.info(f"Generated BigQuery schema with {len(schema)} fields for table {table_name}")
    if not schema:
        logger.error(f"Generated BigQuery schema is empty for table {table_name}!")
    return schema

def get_last_processed_timestamp(client, full_table_id):
    try:
        query = f"""
        SELECT MAX(updated_at) as last_updated_at
        FROM `{full_table_id}`
        """
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            if row.last_updated_at:
                if isinstance(row.last_updated_at, datetime):
                    return row.last_updated_at
                elif isinstance(row.last_updated_at, str):
                    return datetime.strptime(row.last_updated_at, "%Y-%m-%d %H:%M:%S UTC")
        
        logger.info(f"Table {full_table_id} is empty. Starting from the beginning.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)  # Start from Unix epoch, with UTC timezone

    except NotFound:
        logger.info(f"Table {full_table_id} not found. This is an initial load.")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)  # Start from Unix epoch, with UTC timezone

    except Exception as e:
        logger.error(f"Error getting last processed timestamp: {e}")
        return datetime(1970, 1, 1, tzinfo=timezone.utc)  # Start from Unix epoch, with UTC timezone

def identify_datetime_columns(df, schema_info):
    datetime_columns = []
    # Check columns based on pandas dtypes
    datetime_columns.extend(df.select_dtypes(include=['datetime64', 'datetime64[ns]', 'datetime64[ns, UTC]']).columns.tolist())
    
    # Check columns based on MySQL schema information
    for column_name, column_type in schema_info:
        if column_type.lower() in ['datetime', 'timestamp', 'date', 'time']:
            if column_name not in datetime_columns:
                datetime_columns.append(column_name)
    
    # Additional check for string columns that might contain datetime values
    for col in df.select_dtypes(include=['object']):
        if col not in datetime_columns:
            try:
                # Safely convert to string, replacing invalid characters
                safe_strings = df[col].apply(lambda x: str(x).encode('utf-8', errors='replace').decode('utf-8') if x is not None else '')
                
                # Check if column contains any value that looks like a date/time
                if safe_strings.str.match(r'^\d{4}-\d{2}-\d{2}').any() or \
                   safe_strings.str.match(r'^\d{2}:\d{2}:\d{2}').any():
                    datetime_columns.append(col)
            except Exception as e:
                logger.warning(f"Error checking datetime format for column '{col}': {e}")
    
    return datetime_columns


def create_processing_state_table(bigquery_client, dataset_id):
    schema = [
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_updated_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("last_processed_id", "INTEGER", mode="REQUIRED"),
    ]
    
    table_id = f"{dataset_id}.table_processing_state"
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)
    logger.info(f"Created or confirmed existence of table {table_id}")
    
    
    
def process_table_daily_update(table_name, batch_size=10000, local_port=3307, bigquery_client=None):
    try:
        if bigquery_client is None:
            logger.error(f"BigQuery client not provided for table: {table_name}")
            return

        connection = pymysql.connect(
            host="127.0.0.1",
            user=mysql_user,
            password=mysql_password,
            database=mysql_db,
            port=local_port,
            connect_timeout=10,
            charset='utf8mb4',
            use_unicode=True,
            init_command='SET NAMES utf8mb4'
        )
        logger.info(f"Connected to MySQL database for table: {table_name}")

        with connection.cursor() as cursor:
            # Fetch and log the schema
            schema_query = f"DESCRIBE {table_name}"
            cursor.execute(schema_query)
            schema = cursor.fetchall()
            #logger.info(f"Schema for table {table_name}:")
            for column in schema:
                logger.info(f"  {column[0]}: {column[1]}")

            # Get and log the table headers
            headers_query = f"SELECT * FROM {table_name} LIMIT 0"
            cursor.execute(headers_query)
            headers = [d[0] for d in cursor.description]
            # logger.info(f"Headers for table {table_name}: {headers}")

            # Get and log the first row
            # first_row_query = f"SELECT * FROM {table_name} LIMIT 1"
            # cursor.execute(first_row_query)
            # first_row = cursor.fetchone()
            # logger.info(f"First row of table {table_name}:")
            # for header, value in zip(headers, first_row):
            #     logger.info(f"  {header}: {value} (type: {type(value).__name__})")

            # Fetch the schema and sort it
            schema_info = get_mysql_table_schema(cursor, table_name)
            schema_info_sorted = sorted(schema_info, key=lambda x: headers.index(x[0]))
            logger.debug(f"Sorted schema for table {table_name}: {schema_info_sorted}")

            # Create the Pydantic model
            logger.debug("Creating Pydantic model...")
            PydanticModel = create_dynamic_table_model(table_name, schema_info_sorted)
            logger.debug(f"Pydantic model created: {PydanticModel}")
            
            project_id='hexa-data-report-etl-prod'
            dataset_id='cdw_stage'

            bq_table_name = f"neighbourly_{table_name}"
            full_table_id = f"{project_id}.{dataset_id}.{bq_table_name}"
            
            # Generate BigQuery schema
            logger.debug("Generating BigQuery schema...")
            bq_schema = mysql_to_bigquery_schema(table_name, schema_info_sorted)
            logger.info("Generated BigQuery schema:")
            for field in bq_schema:
                logger.info(f"  {field.name}: {field.field_type} (mode: {field.mode})")

            if not bq_schema:
                raise ValueError("Generated BigQuery schema is empty")

            create_table_if_not_exists(bigquery_client, dataset_id, bq_table_name, bq_schema)
            logger.info(f"Ensured BigQuery table {full_table_id} exists with the correct schema")

            try:
                table = bigquery_client.get_table(full_table_id)
                logger.info(f"Verified BigQuery table {full_table_id} exists with {len(table.schema)} columns")
            except Exception as e:
                logger.error(f"Error verifying BigQuery table {full_table_id}: {e}")
                raise
            
            last_updated_at = get_last_processed_timestamp(bigquery_client, full_table_id)
            current_time = datetime.now(timezone.utc)
            buffer_time = timedelta(seconds=10)
            
            
            logger.info(f"Starting processing for table {table_name}")
            logger.info(f"Last updated at: {last_updated_at}")
            logger.info(f"Current time: {current_time}")
            
            while last_updated_at < current_time:
                
                buffered_last_updated_at = last_updated_at - buffer_time
                
                query = f"""
                SELECT *
                FROM {table_name}
                WHERE updated_at > %s
                ORDER BY updated_at, id
                LIMIT {batch_size}
                """

                cursor.execute(query, (buffered_last_updated_at,))
                rows = cursor.fetchall()

                if not rows:
                    logger.info(f"No more data to process for table {table_name}")
                    break

                try:
                    # Clean the data before creating the DataFrame
                    clean_rows = []
                    for row in rows:
                        clean_row = []
                        for value in row:
                            if isinstance(value, bytes):
                                try:
                                    clean_value = value.decode('utf-8')
                                except UnicodeDecodeError:
                                    clean_value = value.decode('utf-8', errors='replace')
                            elif isinstance(value, str):
                                clean_value = value.encode('utf-8', errors='replace').decode('utf-8')
                            else:
                                clean_value = value
                            clean_row.append(clean_value)
                        clean_rows.append(clean_row)
                    num_rows = len(clean_rows)          
                    logger.info(f"Fetched and cleaned {num_rows} rows for table {table_name}")
                     
                    df = pd.DataFrame(clean_rows, columns=headers)
                    logger.info(f"Created DataFrame with shape {df.shape}")
                    
                    datetime_columns = identify_datetime_columns(df, schema_info_sorted)
                    # logger.info(f"Identified datetime columns: {datetime_columns}")

                    # Handle datetime columns
                    for col in datetime_columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S UTC')
                        df[col] = df[col].replace('NaT', None)
                        # logger.info(f"{col} column dtype: {df[col].dtype}")
                        # logger.info(f"Sample {col} values: {df[col].head().tolist()}")
                        # logger.info(f"{col} null count: {df[col].isnull().sum()}")
                                            
                    df = df.where(pd.notna(df), None)
                    
                    data_dicts = df.to_dict(orient='records')
                    logger.info(f"Converted DataFrame to {len(data_dicts)} dictionaries")
                    # logger.info(f"Sample data before loading to BigQuery: {data_dicts[:3]}")

                    
                    validated_rows = validate_data(PydanticModel, data_dicts)
                    logger.info(f"Validated {len(validated_rows)} rows")
                    
                    # sample_size = 3  # Number of samples to log
                    # sample_validated_rows = validated_rows[:sample_size]  # Get the first few validated rows
                    # logger.info(f"Sample validated data: {sample_validated_rows}")


                    if validated_rows:
                        logger.debug(f"First validated row: {validated_rows[0]}")
                        logger.info(f"Attempting to load {len(validated_rows)} validated rows to BigQuery")

                        job_config = bigquery.LoadJobConfig()
                        job_config.schema = bq_schema
                        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

                        temp_table_id = f"{full_table_id}_temp_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

                        try:
                            logger.info(f"Attempting to load {len(validated_rows)} rows to temporary table {temp_table_id}")
                            load_job = bigquery_client.load_table_from_json(validated_rows, temp_table_id, job_config=job_config)
                            load_job.result()
                            logger.info(f"Successfully loaded data to temporary table {temp_table_id}")

                            merge_query = f"""
                            MERGE `{full_table_id}` T
                            USING `{temp_table_id}` S
                            ON T.id = S.id
                            WHEN MATCHED AND S.updated_at > T.updated_at THEN
                            UPDATE SET 
                                {', '.join([f'T.{col} = S.{col}' for col in headers if col not in ['id', 'created_at']])}
                            WHEN NOT MATCHED THEN
                            INSERT ({', '.join(headers)})
                            VALUES ({', '.join([f'S.{col}' for col in headers])})
                            """
                            logger.info(f"Executing merge operation for table {table_name}")
                            logger.debug(f"Merge query: {merge_query}")
                            
                            merge_job = bigquery_client.query(merge_query)
                            merge_job.result()
                            logger.info(f"Merge operation completed successfully for table {table_name}")
                            
                            def parse_datetime(dt_string):
                                if dt_string is None:
                                    return None
                                try:
                                    return datetime.fromisoformat(dt_string)
                                except ValueError:
                                    try:
                                        return datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S UTC")
                                    except ValueError:
                                        try:
                                            return datetime.strptime(dt_string, "%Y-%m-%d").date()
                                        except ValueError:
                                            logger.error(f"Unable to parse datetime: {dt_string}")
                                            return None

                            max_updated_at = max(
                                (parse_datetime(row['updated_at'])
                                for row in validated_rows if row.get('updated_at')),
                                default=last_updated_at
                            )
                            
                            last_updated_at = max(last_updated_at, max_updated_at)
                            logger.info(f"Processed batch up to updated_at: {last_updated_at} for table {table_name}")
                            
                            if num_rows < batch_size:
                                logger.info(f"Fetched less than {batch_size} rows. Assuming all data processed for table {table_name}")
                                break

                        except BadRequest as e:
                            logger.error(f"BadRequest error when loading data to BigQuery: {e}")
                            if hasattr(e, 'errors'):
                                for error in e.errors:
                                    logger.error(f"Detailed error: {error}")
                        except Exception as e:
                            logger.error(f"Error processing batch for table {table_name}: {e}")
                            logger.exception("Detailed traceback:")
                        finally:
                            try:
                                bigquery_client.delete_table(temp_table_id)
                                logger.info(f"Deleted temporary table {temp_table_id}")
                            except NotFound:
                                logger.info(f"Temporary table {temp_table_id} not found, likely already deleted.")
                            except Exception as e:
                                logger.warning(f"Failed to delete temporary table {temp_table_id}: {e}")

                except Exception as e:
                    logger.error(f"Error processing data for table {table_name}: {e}")
                    logger.exception("Detailed traceback:")

        connection.close()
        logger.info(f"MySQL connection closed for table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to process table {table_name}: {e}")
        logger.exception("Detailed traceback:")

# This function is designed to find an available (free) port on local machine.
def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def establish_ssh_tunnel(jump_host, jump_port, jump_username, ssh_pkey, mysql_host, mysql_port):
    local_port = find_free_port()
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            tunnel = SSHTunnelForwarder(
                (jump_host, jump_port),
                ssh_username=jump_username,
                ssh_pkey=ssh_pkey,
                remote_bind_address=(mysql_host, mysql_port),
                local_bind_address=("127.0.0.1", local_port),
            )
            tunnel.start()
            logger.info(f"SSH tunnel established to jump host: {tunnel.local_bind_address}")
            return tunnel, local_port
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_attempts - 1:
                raise
            local_port = find_free_port()
    
    raise Exception("Failed to establish SSH tunnel after multiple attempts")




def main():
    log_start(logger)
    bigquery_client = None
    ssh_tunnel = None

    try:
        # Initialize BigQuery client
        bigquery_client = create_bigquery_client(base64_credentials)
        if not bigquery_client:
            logger.error("Failed to create BigQuery client.")
            sys.exit(1)

        # Load the private key
        try:
            ssh_key_content = neighbourly_private_key
            logger.info(f"SSH key content (first 50 characters): {ssh_key_content[:50]}...")
            ssh_key_file = io.StringIO(ssh_key_content)
            ssh_pkey = paramiko.RSAKey.from_private_key(ssh_key_file)
            logger.info("Private key loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load the private key: {e}")
            sys.exit(1)

        # Establish SSH tunnel
        logger.info(f"Attempting to establish SSH tunnel to {jump_host}:{jump_port}")
        try:
            ssh_tunnel, local_port = establish_ssh_tunnel(
                jump_host, jump_port, jump_username, ssh_pkey, mysql_host, mysql_port
            )
        except Exception as e:
            logger.error(f"Failed to establish SSH tunnel: {str(e)}")
            sys.exit(1)

        # Process tables
        threads = []
        for table_name in tables_to_transfer:
            thread = threading.Thread(
                target=process_table_daily_update, 
                args=(table_name, 500000, local_port, bigquery_client),
                name="Thread-{}".format(table_name)  
            )
            threads.append(thread)
            thread.start()
            logger.info(f"Started processing thread for table: {table_name}")

        # Wait for all threads to complete
        for thread in threads:
            thread.join()
            logger.info(f"Thread for table {thread.name.split('-')[1]} has completed.")

        logger.info("All table processing threads have completed.")

    except Exception as e:
        logger.error(f"An unexpected error occurred in the main function: {e}")
    finally:
        # Clean up resources
        if ssh_tunnel:
            ssh_tunnel.stop()
            logger.info("SSH tunnel closed.")
        if bigquery_client:
            bigquery_client.close()
            logger.info("BigQuery client closed.")
            log_end(logger)

if __name__ == "__main__":
    main()