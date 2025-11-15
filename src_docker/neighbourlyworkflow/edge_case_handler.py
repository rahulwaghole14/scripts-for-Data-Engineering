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

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from neighbourlyworkflow.validation import create_dynamic_table_model, validate_data
from common.bigquery.bigquery import create_bigquery_client, create_table_if_not_exists
from common.logging.logger import logger, log_start, log_end

# Load environment variables
load_dotenv()

# Configure logger
logger = logger(
    "neighbourly_edge_case_handler",
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "log"),
)

# Global variables
project_id = os.environ.get("BIGQUERY_PROJECT_ID")
dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
base64_credentials = os.environ.get("GOOGLE_CLOUD_CRED_BASE64")

# SSH jump host details
jump_host = os.environ.get("NBLY_JUMP_HOST")
jump_port = int(os.environ.get("NBLY_JUMP_PORT", 22))
jump_username = os.environ.get("NBLY_JUMP_USER")
PRIVATE_KEY_PATH = "/Users/amber.wang/.ssh/id_rsa"

# MySQL server details
mysql_host = os.environ.get("NBLY_MYSQL_HOST")
mysql_port = int(os.environ.get("NBLY_MYSQL_PORT", 3306))
mysql_user = os.environ.get("NBLY_MYSQL_USER")
mysql_password = os.environ.get("NBLY_MYSQL_USER_PW")
mysql_db = os.environ.get("NBLY_DB_NAME")

# Define the list of tables to be transferred
tables_to_transfer = [
    'notification_preference',
    'paf_address'
    # ,
    #  'user_paf_address',
    #  'user',
    #  'neighbourhood'
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
            # Check if column contains any value that looks like a date/time
            if df[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}').any() or \
               df[col].astype(str).str.match(r'^\d{2}:\d{2}:\d{2}').any():
                datetime_columns.append(col)
    
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
    
def safe_datetime_conversion(x):
    if pd.isna(x) or x == '' or x == '0000-00-00' or x == '0000-00-00 00:00:00':
        return None
    try:
        dt = pd.to_datetime(x, errors='coerce')
        if pd.isna(dt) or dt is pd.NaT:
            return None
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except:
        return None  
    
def process_edge_cases(table_name, batch_size=10000, local_port=3307, bigquery_client=None):
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
        )
        logger.info(f"Connected to MySQL database for table: {table_name}")

        with connection.cursor() as cursor:
            # Fetch and log the schema
            schema_query = f"DESCRIBE {table_name}"
            cursor.execute(schema_query)
            schema = cursor.fetchall()
            logger.info(f"Schema for table {table_name}:")
            for column in schema:
                logger.info(f"  {column[0]}: {column[1]}")

            # Get and log the table headers
            headers_query = f"SELECT * FROM {table_name} LIMIT 0"
            cursor.execute(headers_query)
            headers = [d[0] for d in cursor.description]

            # Fetch the schema and sort it
            schema_info = get_mysql_table_schema(cursor, table_name)
            schema_info_sorted = sorted(schema_info, key=lambda x: headers.index(x[0]))
            logger.debug(f"Sorted schema for table {table_name}: {schema_info_sorted}")

            # Create the Pydantic model
            logger.debug("Creating Pydantic model...")
            PydanticModel = create_dynamic_table_model(table_name, schema_info_sorted)
            logger.debug(f"Pydantic model created: {PydanticModel}")

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

            last_processed_id = 0  
            # Query for edge cases
            while True:
                try:
                    query = f"""
                    SELECT *
                    FROM {table_name}
                    WHERE id > {last_processed_id}
                    AND (updated_at IS NULL 
                        OR updated_at = '0000-00-00 00:00:00'
                        OR updated_at = '0000-00-00'
                        OR CHAR_LENGTH(TRIM(updated_at)) = 0)
                    ORDER BY id
                    LIMIT {batch_size}
                    """

                    cursor.execute(query)
                    rows = cursor.fetchall()

                    if not rows:
                        logger.info(f"No more edge cases to process for table {table_name}")
                        break

                    num_rows = len(rows)
                    logger.info(f"Fetched {num_rows} edge case rows for table {table_name}")               
                    df = pd.DataFrame(rows, columns=headers)
                    logger.info(f"Created DataFrame with shape {df.shape}")

                    datetime_columns = identify_datetime_columns(df, schema_info_sorted)
                    logger.info(f"Identified datetime columns: {datetime_columns}")

                    # Handle datetime columns
                    for col in datetime_columns:
                        before_conversion = df[col].value_counts(dropna=False).head()
                        df[col] = df[col].apply(safe_datetime_conversion)
                        after_conversion = df[col].value_counts(dropna=False).head()
                        logger.info(f"Column {col} before conversion: {before_conversion}")
                        logger.info(f"Column {col} after conversion: {after_conversion}")

                    df = df.where(pd.notna(df), None)

                    data_dicts = df.to_dict(orient='records')
                    validated_rows = validate_data(PydanticModel, data_dicts)
                    logger.info(f"Validated {len(validated_rows)} rows")

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
                            WHEN MATCHED THEN
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
                            last_processed_id = max(max(row['id'] for row in validated_rows), df['id'].max())
                            logger.info(f"Updated last processed ID to: {last_processed_id}")

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
                    else:
                        # Even if no rows were validated, we should still update the last_processed_id
                        last_processed_id = df['id'].max()
                        logger.info(f"No valid rows to process. Updated last processed ID to: {last_processed_id}")
                except Exception as e:
                    logger.error(f"Error processing batch for table {table_name}: {e}")
                    logger.exception("Detailed traceback:")
                    # Decide whether to break or continue based on your error handling strategy
                    # break

        logger.info(f"Completed processing edge cases for table {table_name}")   

    except Exception as e:
        logger.error(f"Error processing data for table {table_name}: {e}")
        logger.exception("Detailed traceback:")

    finally:
        if 'connection' in locals() and connection:
            connection.close()
            logger.info(f"MySQL connection closed for table: {table_name}")

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

        # Check if PRIVATE_KEY_PATH is set
        if not PRIVATE_KEY_PATH:
            logger.error("Private Key Path is not set. Please check your .env file.")
            sys.exit(1)

        # Load the private key
        try:
            ssh_pkey = paramiko.RSAKey(filename=PRIVATE_KEY_PATH)
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
                target=process_edge_cases, 
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