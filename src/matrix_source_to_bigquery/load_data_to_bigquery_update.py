

"""Load matrix data to bigquery """
import os
from datetime import datetime, timedelta
from decimal import Decimal
import hashlib
import threading
import time
import pyodbc
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account
# Import your custom logging, config loading, and dotenv as before
from logger import get_logger
from load_config import load_config
from dotenv import load_dotenv
# Import the separated logic for Pydantic models
from model_generator import fetch_and_create_dynamic_model, validate_data_with_pydantic


# Load environment variables
load_dotenv()

logger = get_logger("matrix_data_transfer_to_bigQuery")

class Watchdog(threading.Thread):
    def __init__(self, timeout, callback):
        super().__init__()
        self.timeout = timeout
        self.callback = callback
        self.reset()
        self.daemon = True  # Allow the program to exit even if watchdog is still running
        self.start()

    def reset(self):
        self.last_active = time.time()

    def run(self):
        while True:
            time.sleep(self.timeout / 2)
            if time.time() - self.last_active > self.timeout:
                self.callback()
                break

def create_bigquery_client():
    config = load_config()
    google_cred = config['google_cred']
    try:
        credentials = service_account.Credentials.from_service_account_info(
            google_cred,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        logger.info("Created BigQuery client")
    except Exception as error:
        logger.error(f"Failed to create BigQuery client: {error}")
        client = None
    return client

def convert_to_serializable(value):
    if isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, Decimal):
        # Convert Decimal to string to preserve precision
        return str(value)
        # Or convert to float if you're okay with potential precision loss
        # return float(value)
    elif isinstance(value, str) and len(value) > 1048576:  # Example limit
        logger.warning(f"String exceeds max length with size {len(value)} bytes. Truncating...")
        return value[:1048576]  # Truncate the string
    # Handle other special types as necessary
    return value


def prepare_data_for_bigquery(validated_data, include_hash_key=False):
    """
    Prepare validated data for BigQuery, converting values as necessary.
    """
    data_for_bigquery = []
    for row in validated_data:
        # Convert values to a serializable format
        processed_row = {k: convert_to_serializable(v) for k, v in row.items()}
        
        if include_hash_key:
            # Generate a hash key based on the values of the row
            hash_key = generate_hash_key(row.values())
            processed_row['hash_key'] = hash_key  # Add hash key to the row data only if required
        processed_row['update_time'] = datetime.now(pytz.utc).isoformat()
        data_for_bigquery.append(processed_row)
    return data_for_bigquery

def generate_hash_key(values):
    """
    Generate a hash key for a list of values.
    """
    # Create a concatenated string representation of the values
    concatenated_values = ''.join(str(v) for v in values)
    # Generate and return a SHA-256 hash of the concatenated string
    return hashlib.sha256(concatenated_values.encode()).hexdigest()


def sanitize_table_name(schema_name, table_name):
    """Sanitize table names for BigQuery."""
    # Only prefix the schema name if it's not 'dbo' and replace '.' with '_'
    return f"{schema_name}_{table_name}".replace('.', '_') if schema_name != 'dbo' else table_name


def get_table_schema(client, dataset_name, schema_name, table_name):
    """
    Fetch the schema of a BigQuery table.

    Parameters:
    - client: BigQuery client object
    - dataset_name: Name of the dataset
    - table_name: Name of the table

    Returns:
    - A list of bigquery.SchemaField objects representing the table schema
    """
    bq_table_name = sanitize_table_name(schema_name, table_name)

    table_id = f"{client.project}.{dataset_name}.{bq_table_name}"
    table = client.get_table(table_id)  # API request
    return table.schema

def get_last_processed_pk(client, dataset_name, schema_name, table_name):

    bq_table_name = sanitize_table_name(schema_name, table_name)

    query = f"""
    SELECT last_processed_pk
    FROM `{client.project}.{dataset_name}.batch_metadata`
    WHERE table_name = '{bq_table_name}'
    LIMIT 1
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row.last_processed_pk
    return None  # Return None if no entry exists for the table

def get_last_processed_date(client, dataset_name, schema_name, table_name):
    bq_table_name = sanitize_table_name(schema_name, table_name)
    query = f"""
    SELECT last_processed_date
    FROM `{client.project}.{dataset_name}.batch_metadata`
    WHERE table_name = '{bq_table_name}'
    LIMIT 1
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row.last_processed_date
    return None  # Return None if no entry exists for the table



def update_last_processed_info(client, dataset_name, schema_name, table_name, last_processed_value):
    bq_table_name = sanitize_table_name(schema_name, table_name)
    
    # Determine the column and value to update based on the type of last_processed_value
    if isinstance(last_processed_value, datetime):
        column_name = 'last_processed_date'
        formatted_datetime = last_processed_value.isoformat("T") + "Z"
        value_to_set = f"TIMESTAMP '{formatted_datetime}'"  # Use TIMESTAMP type literal for BigQuery
    else:
        column_name = 'last_processed_pk'
        value_to_set = last_processed_value  # Assuming this is an integer or a string that doesn't need additional formatting


    query = f"""
    MERGE `{client.project}.{dataset_name}.batch_metadata` AS target
    USING (SELECT '{bq_table_name}' as table_name, {value_to_set} as {column_name}) AS source
    ON target.table_name = source.table_name
    WHEN MATCHED THEN
        UPDATE SET {column_name} = source.{column_name}
    WHEN NOT MATCHED THEN
        INSERT (table_name, {column_name}) VALUES (source.table_name, source.{column_name});
    """
    client.query(query).result()


def construct_merge_statement(client, schema, dataset_name, table_name, temp_table_name, primary_key_column=None):
    """
    Dynamically construct a MERGE SQL statement for BigQuery that includes handling of the hash_key,
    using a temporary table as the source. The statement updates or inserts rows based on the hash_key
    and primary key (if provided), and sets update_time to the current timestamp on updates.
    """
    columns = [field.name for field in schema if field.name not in ('update_time', 'hash_key')]  # Exclude control columns
    all_columns = columns + ['hash_key', 'update_time']  # Include 'hash_key' and 'update_time' for INSERT and UPDATE

    insert_columns = ", ".join(all_columns)
    insert_values = ", ".join([f"SOURCE.{col}" for col in columns] + ['SOURCE.hash_key', 'CURRENT_TIMESTAMP()'])

    on_condition = f"ON TARGET.{primary_key_column} = SOURCE.{primary_key_column}" if primary_key_column else "ON TARGET.hash_key = SOURCE.hash_key"

    update_set = ", ".join([f"TARGET.{col} = SOURCE.{col}" for col in columns] + ["TARGET.hash_key = SOURCE.hash_key", "TARGET.update_time = CURRENT_TIMESTAMP()"])

    if primary_key_column:
        match_condition = f"WHEN MATCHED AND TARGET.hash_key != SOURCE.hash_key THEN UPDATE SET {update_set}"
    else:
        # For tables without a primary key, we assume every row needs to be compared by hash_key,
        # and only update if the hash_key has changed.
        match_condition = f"WHEN MATCHED THEN UPDATE SET {update_set}"

    not_matched_condition =f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"

    merge_sql = f"""
    MERGE `{client.project}.{dataset_name}.{table_name}` AS TARGET
    USING `{client.project}.{dataset_name}.{temp_table_name}` AS SOURCE
    {on_condition}
    {match_condition}
    {not_matched_condition};
    """
    return merge_sql




def upload_data_to_bigquery(client, dataset_name, table_name, data, max_retries=5, initial_retry_delay=10, max_retry_delay=120):
    table_id = f"{client.project}.{dataset_name}.{table_name}"
    attempt = 0
    retry_delay = initial_retry_delay

    while attempt < max_retries:
        try:
            errors = client.insert_rows_json(table_id, data)
            if errors:
                logger.error(f"Attempt {attempt+1}: Failed to upload data to BigQuery table {table_name}: {errors}")
                attempt += 1
                time.sleep(retry_delay)  # Wait before retrying
            else:
                logger.info(f"Successfully uploaded {len(data)} rows to BigQuery table {table_name}.")
                return len(data)  # Return the number of rows uploaded
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}: Exception during data upload to {table_name}: {str(e)}")
            attempt += 1
            time.sleep(retry_delay)  # Wait before retrying
            
            # Calculate next delay with exponential backoff, capped by max_retry_delay
            retry_delay = min(retry_delay * 2, max_retry_delay)
    
    logger.error(f"Failed to upload data to BigQuery table {table_name} after {max_retries} attempts.")
    return 0  # Indicate failure by returning 0



def load_full_refresh_data_to_bigquery(cursor, client, dataset_name, schema_name, table_name, batch_size=1000):
    """
    Loads data from SQL Server to BigQuery for tables that require a full refresh,
    including validation of each row using dynamically generated Pydantic models.
    """
    logger.info(f"Starting full refresh for table {table_name}.")
    # Generate a dynamic Pydantic model based on the table schema
    dynamic_model = fetch_and_create_dynamic_model(cursor, schema_name, table_name)

    bq_table_name = sanitize_table_name(schema_name, table_name)

    truncate_bigquery_table(client, dataset_name, bq_table_name)

    query = f"SELECT * FROM {schema_name}.{table_name}"  # Adjust SQL query as needed
    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            logger.info(f"No more data to fetch for {table_name}.")
            break
        
        validated_data_for_bigquery = []
        for row in rows:
            # Convert the row to a dictionary
            row_data = {desc[0]: row[i] for i, desc in enumerate(cursor.description)}
            
            # Validate the row using the dynamic model
            validated_row = validate_data_with_pydantic(dynamic_model, row_data)
            if validated_row:
                # If row is valid, prepare it for BigQuery
                prepared_row = prepare_data_for_bigquery([validated_row], include_hash_key=False)
                validated_data_for_bigquery.extend(prepared_row)
            else:
                # Log validation failure or take appropriate action
                logger.error(f"Validation failed for row in table {table_name}: {row_data}")

        # Upload validated and prepared data to BigQuery
        if validated_data_for_bigquery:
            success = upload_data_to_bigquery(client, dataset_name, bq_table_name, validated_data_for_bigquery)
            if success:
                logger.info(f"Uploaded {len(validated_data_for_bigquery)} validated rows to BigQuery table {table_name}.")
            else:
                logger.error(f"Failed to upload validated data for table {table_name}.")

def load_data_from_sql_server(cursor, schema_name, table_name, primary_key_column, columns_to_include, non_unique_key_column = None, last_processed_value=None, is_date_based=False, batch_size=4000):
    if table_name in columns_to_include:
        columns_list = ', '.join(columns_to_include[table_name])
    else:
        columns_list = '*'
    key_columns = primary_key_column or non_unique_key_column

    # Initialize where_clause to an empty string to ensure it has a value before being used
    where_clause = ""

    if is_date_based and last_processed_value:
        where_clause = f"WHERE Retail_SaleDate > '{last_processed_value.strftime('%Y-%m-%d')}'"
    elif not is_date_based and last_processed_value:
        where_clause = f"WHERE {key_columns} > '{last_processed_value}'"

    if table_name == 'tbl_RPT_TM1retail':
        date_limit = datetime.now() - timedelta(days=24*30)
        date_clause = f"Retail_SaleDate >= '{date_limit.strftime('%Y-%m-%d')}'"
        where_clause = f"{where_clause} AND {date_clause}" if where_clause else f"WHERE {date_clause}"

    if table_name == 'tbl_ObjectLink':
        link_clause = "ToDate IS NULL AND LinkObject IS NOT NULL"
        where_clause = f"{where_clause} AND {link_clause}" if where_clause else f"WHERE {link_clause}"

    order_by_clause = f"ORDER BY {key_columns if key_columns else 'Retail_SaleDate'}"

    query = f"SELECT {columns_list} FROM {schema_name}.{table_name} {where_clause} {order_by_clause}"
    logger.info(f"Executing query for {table_name}: {query}")

    cursor.execute(query)
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            logger.info(f"No more data to fetch for {table_name}.")
            break
        logger.info(f"Fetched {len(rows)} rows from {table_name}.")
        yield rows



def truncate_bigquery_table(client, dataset_name,table_name):

    query = f"TRUNCATE TABLE `{client.project}.{dataset_name}.{table_name}`"
    try:
        client.query(query).result()
        logger.info(f"Table {table_name} truncated successfully.")
    except Exception as e:
        logger.error(f"Failed to truncate table {table_name}: {e}")

def update_bigquery_with_hash_keys(client, dataset_name, schema_name, table_name, primary_key_column, temp_table_name, max_retries=5, initial_retry_delay=10, max_retry_delay=120):
    
    bq_table_name = sanitize_table_name(schema_name, table_name)

    schema = get_table_schema(client, dataset_name, schema_name, table_name)
    merge_sql = construct_merge_statement(client, schema, dataset_name, bq_table_name, primary_key_column, temp_table_name)
    attempt = 0
    retry_delay = initial_retry_delay

    while attempt < max_retries:
        try:
            # Execute the MERGE operation and directly use the returned job for statistics
            job = client.query(merge_sql)
            job.result()  # Wait for the job to complete
            
            # Access num_dml_affected_rows directly from the job object
            if hasattr(job, 'num_dml_affected_rows'):
                logger.info(f"Data for {table_name} merged based on hash keys. Job ID: {job.job_id}, Rows affected: {job.num_dml_affected_rows}")
            else:
                logger.info(f"Data for {table_name} merged based on hash keys. Job ID: {job.job_id}, Rows affected: N/A")
                
            return True
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}: Failed to merge data for {table_name}: {e}")
            attempt += 1
            time.sleep(retry_delay)  # Wait before retrying
            
            # Calculate next delay with exponential backoff, capped by max_retry_delay
            retry_delay = min(retry_delay * 2, max_retry_delay)
            
            if attempt < max_retries:
                logger.info(f"Retrying merge operation for {table_name} after {retry_delay} seconds...")

    logger.error(f"Failed to merge data for {table_name} after {max_retries} attempts.")
    return False

def generate_temp_table_name(base_table_name):
    timestamp = int(time.time())
    return f"{base_table_name}_temp_{timestamp}"

def create_temp_table(client, dataset_name, schema_name, table_name, schema):

    base_table_name = sanitize_table_name(schema_name, table_name)
 
    temp_table_name = generate_temp_table_name(base_table_name)
    table_id = f"{client.project}.{dataset_name}.{temp_table_name}"
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # API request
    logger.info(f"Created temporary table {table_id}")
    return temp_table_name

def drop_temp_table(client, dataset_name, temp_table_name):
    table_id = f"{client.project}.{dataset_name}.{temp_table_name}"
    client.delete_table(table_id)  # API request
    logger.info(f"Dropped temporary table {table_id}")



def main():

    def watchdog_callback():
        logger.error("Script is stuck. Taking action.")
        # Implement additional recovery or notification logic here

    # Set up the watchdog with a 10-minute timeout and a callback function
    watchdog = Watchdog(timeout=600, callback=watchdog_callback)

    client = create_bigquery_client()
    if client is None:
        return
    
    sql_server = os.getenv('MATRIX_DB_SERVER')
    sql_database = os.getenv('MATRIX_DB_DATABASE')
    sql_username = os.getenv('MATRIX_DB_USERNAME')
    sql_password = os.getenv('MATRIX_DB_PASSWORD')
    sql_port = os.getenv('MATRIX_DB_PORT')

    dataset_name = 'cdw_stage_matrix'

    full_refresh_tables = [
                            ('dbo', 'tbl_country'), 
                            ('dbo', 'tbl_service'), 
                            ('dbo', 'tbl_product'), 
                            ('dbo', 'tbl_period'), 
                            ('dbo', 'cancellation_reason'),
                            ('ffx', 'temp_winback_subscribers1'),
                            ('dbo', 'rate_header'),
                            ('dbo', 'tbl_RateStructure'),
                            ('dbo', 'tbl_RateStructureItem'),
                            ('dbo', 'tbl_RateItem'),
                            ('dbo', 'TBL_LINKType')
                            ]
    
    hash_key_tables = [  
                        ('dbo', 'tbl_person'), 
                        ('dbo', 'tbl_ContactNumber'), 
                        ('dbo', 'tbl_location'), 
                        ('dbo', 'subscriber'), 
                        ('dbo', 'subscription'), 
                        ('dbo', 'subord_cancel'),
                        ('dbo', 'complaints'),
                        ('dbo',	'tbl_ObjectLink'),
                        ('ffx',	'tbl_RPT_TM1retail'),
                        ('ffx', 'vw_RPT_SubsRateRounds')
                        ]

    primary_key_columns = {
        ('dbo','complaints'): 'comp_id',
        ('dbo','subscriber'): 'subs_pointer',
        ('dbo','subscription'): 'sord_pointer',
        ('dbo','tbl_person'): 'person_pointer',
        ('dbo','tbl_location'): 'loc_pointer',  
        ('dbo','tbl_ContactNumber'): 'ContactPointer', 
        ('dbo','subord_cancel'): 'ObjectPointer', 
        ('dbo',	'tbl_ObjectLink'): 'ObjectLinkPointer',
        ('ffx',	'tbl_RPT_TM1retail'): None,
        ('ffx',	'vw_RPT_SubsRateRounds'): None
    }

    non_unique_key_columns = {
        ('ffx',	'vw_RPT_SubsRateRounds'): 'Subs_id'
    }

    is_date_based_processing = {
        'tbl_RPT_TM1retail': True
    }

    is_non_unique_key_based_processing = {
        'vw_RPT_SubsRateRounds': True
    }

    columns_to_include = {'tbl_ObjectLink': ['Level1Pointer', 'LinkObject', 'LinkType','ToDate', 'ObjectLinkPointer']}
   
    # truancate the batch_metadata table for full table comparison everytime when running this script
    truncate_bigquery_table(client, dataset_name, 'batch_metadata')

    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server},{sql_port};DATABASE={sql_database};UID={sql_username};PWD={sql_password};"
    conn = pyodbc.connect(conn_string)
    cursor = conn.cursor()

   # Full refresh tables
    for schema_name, table_name in full_refresh_tables:
        load_full_refresh_data_to_bigquery(cursor, client, dataset_name,schema_name, table_name)
      
                
    for schema_name, table_name in hash_key_tables:

        dynamic_model = fetch_and_create_dynamic_model(cursor, schema_name, table_name)
        primary_key_column = primary_key_columns.get((schema_name, table_name), None)
        non_unique_key_column = non_unique_key_columns.get((schema_name, table_name), None)
        is_date_based = is_date_based_processing.get(table_name, False)
        is_non_unique_key_based = is_non_unique_key_based_processing.get(table_name, False)
            
        if is_date_based:
                last_processed_date = get_last_processed_date(client, dataset_name, schema_name, table_name)
                logger.info(f"Starting to process {table_name} with last processed date: {last_processed_date}")
        else:
            last_processed_pk = get_last_processed_pk(client, dataset_name, schema_name, table_name) or 0
            logger.info(f"Starting to process {table_name} with last processed PK: {last_processed_pk}")

        schema = get_table_schema(client, dataset_name, schema_name, table_name)
        temp_table_name = create_temp_table(client, dataset_name, schema_name, table_name, schema)
    
    # Initialize the last_processed_value based on the type of processing
        last_processed_value = get_last_processed_date(client, dataset_name, schema_name, table_name) if is_date_based else get_last_processed_pk(client, dataset_name, schema_name, table_name)

        for batch_rows in load_data_from_sql_server(cursor, schema_name, table_name, primary_key_column, columns_to_include, non_unique_key_column, last_processed_value, is_date_based):
            processed_data = []
            for row in batch_rows:
                row_data = {desc[0]: row[i] for i, desc in enumerate(cursor.description)}
                validated_row = validate_data_with_pydantic(dynamic_model, row_data)
                if validated_row:
                    processed_data.append(validated_row)
            
            if processed_data:
                logger.info(f"Processing batch of size: {len(processed_data)}")
                data_for_bigquery = prepare_data_for_bigquery(processed_data, include_hash_key=True)
                success = upload_data_to_bigquery(client, dataset_name, temp_table_name, data_for_bigquery)
                if success:
                    logger.info(f"Batch uploaded successfully, number of rows: {success}")
                else:
                    logger.error("Batch upload failed.")
            
            if is_date_based:
                last_processed_date = max(row['Retail_SaleDate'] for row in processed_data)
                update_last_processed_info(client, dataset_name, schema_name, table_name, last_processed_date)
                logger.info(f"Last processed date updated to: {last_processed_date}")
            else:
                if is_non_unique_key_based:
                    last_processed_pk = max(row[non_unique_key_column] for row in processed_data)
                else:
                    last_processed_pk = max(row[primary_key_column] for row in processed_data)
                update_last_processed_info(client, dataset_name, schema_name, table_name, last_processed_pk)
                logger.info(f"Last processed PK updated to: {last_processed_pk}")


        logger.info(f"Starting merge into final table: {table_name} from temporary table: {temp_table_name}")
        update_bigquery_with_hash_keys(client, dataset_name, schema_name, table_name, temp_table_name, primary_key_column)
        if is_date_based:
            last_processed_date = max(row['Retail_SaleDate'] for row in processed_data)
            update_last_processed_info(client, dataset_name, schema_name, table_name, last_processed_date)
            logger.info(f"Last processed date updated to: {last_processed_date}")
        else:
            if is_non_unique_key_based:
                last_processed_pk = max(row[non_unique_key_column] for row in processed_data)
            else:
                last_processed_pk = max(row[primary_key_column] for row in processed_data)
            update_last_processed_info(client, dataset_name, schema_name, table_name, last_processed_pk)
            logger.info(f"Last processed PK updated to: {last_processed_pk}")


        drop_temp_table(client, dataset_name, temp_table_name)
        logger.info(f"Temporary table dropped: {temp_table_name}")
        watchdog.reset()
                        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
