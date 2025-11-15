""" some helpful functions for bigquery """

import json
import base64
import logging
import pandas as pd
import os
from datetime import date, datetime, time
from typing import List, get_origin, get_args, Union, Type
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import ClientError

from common.validation.validators import MyBaseModel


class UnsupportedTypeError(Exception):
    """hello"""


def is_pydantic_model(some_type: Type) -> bool:
    """
    Check if the given type is a subclass of MyBaseModel.
    """
    return isinstance(some_type, type) and issubclass(some_type, MyBaseModel)


def run_bq_sql(client, sql):
    """run sql command in bq"""
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        logging.info("Sql query completed successfully.")
    except ClientError as err:
        logging.error("run_bq_sql(): %s", query_job.errors)
        raise err
    except Exception as err:
        logging.error("Sql query operation failed: %s", str(err))
        raise


def return_bq_sql_dict(client, sql):
    """Run SQL command in BigQuery and return the results."""
    try:
        query_job = client.query(sql)
        results = query_job.result()  # Wait for the query to complete
        logging.info("SQL query completed successfully.")
        return [
            dict(row) for row in results
        ]  # Convert results to a list of dictionaries
    except ClientError as err:
        logging.error("return_bq_sql_dict(): %s", query_job.errors)
        raise err
    except Exception as err:
        logging.error("SQL query operation failed: %s", str(err))
        raise


def generate_bq_merge_statement(
    target_table, temp_table, schema, primary_keys
):
    """
    Generate a SQL MERGE statement
    to merge data from a temporary table into the target table.

    Args:
    - target_table (str): The fully-qualified BigQuery target table name.
    - temp_table (str): The fully-qualified BigQuery temporary table name.
    - schema (List[bigquery.SchemaField]): The BigQuery schema list.
    - primary_keys (List[str]):
        A list of field names that constitute the primary key.

    Returns:
    - str: A SQL MERGE statement.
    """
    # Generate the ON clause for matching primary keys
    on_clause = " AND ".join(
        [f"TARGET.{pk} = TEMP.{pk}" for pk in primary_keys]
    )

    # Generate the WHEN MATCHED THEN UPDATE SET clause
    update_clause = ", ".join(
        [
            f"{field.name} = TEMP.{field.name}"
            for field in schema
            if field.name not in primary_keys
        ]
    )

    # Generate the WHEN NOT MATCHED BY TARGET THEN INSERT clause
    insert_fields = ", ".join([field.name for field in schema])
    insert_values = ", ".join([f"TEMP.{field.name}" for field in schema])

    merge_sql = f"""
    MERGE `{target_table}` AS TARGET
    USING `{temp_table}` AS TEMP
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_clause}
    WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_fields})
    VALUES ({insert_values})
    """

    return merge_sql


def map_basic_type_to_bq_field(
    field_name: str,
    field_type: type,
    mode: str = "NULLABLE",
    type_mapping: dict = None,
) -> bigquery.SchemaField:
    """
    Map Python basic types to BigQuery field types and create a SchemaField.

    Args:
    - field_name (str): The name of the field.
    - field_type (type): The Python type of the field.
    - mode (str): The mode of the field in BigQuery
        ('NULLABLE', 'REQUIRED', 'REPEATED').

    Returns:
    - bigquery.SchemaField:
        A BigQuery SchemaField corresponding to the Python type.
    """
    # Define a mapping from Python types to BigQuery types
    if type_mapping is None:  # Default type mapping

        type_mapping = {
            int: "INTEGER",
            str: "STRING",
            float: "FLOAT",
            bool: "BOOLEAN",
            datetime: "TIMESTAMP",
            date: "DATE",
            time: "TIME",
        }

    # Look up the BigQuery type in the mapping
    # Default to STRING for unsupported/unknown types
    bq_type = type_mapping.get(field_type, "STRING")

    # Create and return the SchemaField
    return bigquery.SchemaField(name=field_name, field_type=bq_type, mode=mode)


def pydantic_model_to_bq_schema(
    model: Type[BaseModel],
    type_mapping: dict = None,
) -> List[bigquery.SchemaField]:
    """
    Recursively create BigQuery schema from a Pydantic model,
    ensuring complex fields are appropriately marked as RECORD types.
    """
    return_schema = []
    for field_name, field_type in model.__annotations__.items():
        # Extract the actual type and check if it's
        # a List (for REPEATED fields) or another complex type
        origin = get_origin(field_type)
        args = get_args(field_type)

        if origin is Union:
            # Handling Optional fields by filtering out NoneType
            non_none_types = [arg for arg in args if arg is not type(None)]
            field_type = non_none_types[0] if non_none_types else field_type
            origin = get_origin(field_type)
            args = get_args(field_type)

        if origin in [list, List] and args:
            # List handling, checking if it's a list of complex types/models
            item_type = args[0]
            if is_pydantic_model(item_type):
                # handle REPEATED RECORDs
                nested_fields = pydantic_model_to_bq_schema(item_type)
                field_schema = bigquery.SchemaField(
                    name=field_name,
                    field_type="RECORD",
                    fields=nested_fields,
                    mode="REPEATED",
                )
            else:
                # use STRING for REPEATED basic types
                field_schema = bigquery.SchemaField(
                    name=field_name, field_type="STRING", mode="REPEATED"
                )
        elif is_pydantic_model(field_type):
            # Handling nested Pydantic models as RECORD
            nested_fields = pydantic_model_to_bq_schema(field_type)
            field_schema = bigquery.SchemaField(
                name=field_name,
                field_type="RECORD",
                fields=nested_fields,
                mode="NULLABLE",
            )
        else:
            # Handling basic types
            field_schema = map_basic_type_to_bq_field(
                field_name,
                field_type,
                type_mapping=type_mapping,
            )

        return_schema.append(field_schema)

    return return_schema


def create_bigquery_client(base64_credentials):
    """create bigquery client"""
    try:
        encoded_cred = (
            base64_credentials  # os.environ.get("GOOGLE_CLOUD_CRED_BASE64")
        )
        decoded_cred = base64.b64decode(encoded_cred).decode("utf-8")
        google_cred = json.loads(decoded_cred)

        credentials = service_account.Credentials.from_service_account_info(
            google_cred,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        authenticated_client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

        logging.info("Bigquery client created in create_bigquery_client()")
    except Exception as error:  # pylint: disable=broad-except
        logging.info("Error in create_bigquery_client(): %s", error)

    return authenticated_client

def create_bigquery_client_dev(google_cred):
    """create bigquery client"""
    try:

        credentials = service_account.Credentials.from_service_account_info(
            google_cred,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        authenticated_client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

        logging.info("Bigquery client created in create_bigquery_client()")
    except Exception as error:  # pylint: disable=broad-except
        logging.info("Error in create_bigquery_client(): %s", error)

    return authenticated_client

def create_or_update_table_schema(
    client, dataset_id, table_id, desired_schema
):
    """Create BQ table or update its schema by adding new fields, including nested fields."""
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)  # Attempt to get the table
        current_schema = table.schema

        # Function to recursively update schema
        def update_schema(current_fields, desired_fields):
            current_field_map = {field.name: field for field in current_fields}
            updated_fields = []

            for desired_field in desired_fields:
                if desired_field.name in current_field_map:
                    current_field = current_field_map[desired_field.name]
                    if (
                        desired_field.field_type == "RECORD"
                        and desired_field.fields
                    ):
                        # Recursively update nested fields
                        updated_nested_fields = update_schema(
                            current_field.fields, desired_field.fields
                        )
                        # Create a new field with updated nested fields
                        updated_field = bigquery.SchemaField(
                            name=desired_field.name,
                            field_type=desired_field.field_type,
                            mode=desired_field.mode,
                            fields=updated_nested_fields,
                            description=desired_field.description,
                        )
                        updated_fields.append(updated_field)
                    else:
                        # Keep the existing field
                        updated_fields.append(current_field)
                else:
                    # Add new field
                    logging.info(
                        "Adding new field to schema: %s", desired_field.name
                    )
                    updated_fields.append(desired_field)
            return updated_fields

        # Update the schema recursively
        updated_schema = update_schema(current_schema, desired_schema)

        # Check if the schema was updated
        if updated_schema != current_schema:
            table.schema = updated_schema
            client.update_table(table, ["schema"])
            logging.info(
                "Updated BigQuery table schema: %s.%s", dataset_id, table_id
            )
        else:
            logging.info(
                "No schema update required for BigQuery table: %s.%s",
                dataset_id,
                table_id,
            )

    except NotFound:
        table = bigquery.Table(table_ref, schema=desired_schema)
        client.create_table(table)
        logging.info(
            "Bigquery created table: %s.%s create_table_if_not_exists()",
            table_ref.dataset_id,
            table_ref.table_id,
        )


def create_table_if_not_exists(
    client, dataset_id, table_id, schema, partition_field=None
):
    """
    Create BigQuery table if it doesn't exist,
    optionally with partitioning on a specified field.
    """

    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        client.get_table(table_ref)
        logging.info(
            "BQ table already exists: %s.%s create_table_if_not_exists()",
            table_ref.dataset_id,
            table_ref.table_id,
        )
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        if partition_field:
            logging.info("Partitioning table on field: %s", partition_field)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field,
            )
        client.create_table(table)
        logging.info(
            "BigQuery created table: %s.%s create_table_if_not_exists()",
            table_ref.dataset_id,
            table_ref.table_id,
        )


def load_data_to_bigquery(
    client, dataset_id, table_id, records, dimensions, schema
):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    """
    Load list of dicts into a BQ table with dimension-based deduplication,
    after verifying all records share the same specified dimensions.

    Parameters:
    - client: BigQuery Client object
    - dataset_id: Dataset ID where the table resides
    - table_id: Table ID into which data is to be loaded
    - records: List of dictionaries representing the records to be loaded
    - dimensions: List of dimension names as strings for deduplication criteria
    - schema: BigQuery schema representing the table's structure
    """
    if not records:
        logging.info("No records to load.")
        return

    # Check if all records have the same dimensions
    dimension_values = {dim: set() for dim in dimensions}
    for record in records:
        for dim in dimensions:
            if dim in record:
                dimension_values[dim].add(record[dim])
            else:
                logging.error("Dimension %s missing in a record.", dim)
                return

    # If dimension set has more than one value, records have diff dimensions
    if any(len(values) > 1 for values in dimension_values.values()):
        logging.error("Not all records have the same dimensions.")
        return

    # Convert date objects to ISO format strings
    for record in records:
        for key, value in record.items():
            if isinstance(value, date):
                record[key] = value.isoformat()

    # Since dimensions are the same for all records, use first record for check
    first_record = records[0]
    where_clause = " AND ".join([f"{dim} = @{dim}" for dim in dimensions])
    query_params = [
        bigquery.ScalarQueryParameter(dim, "STRING", first_record[dim])
        for dim in dimensions
    ]

    query = (
        f"SELECT 1 FROM `{dataset_id}.{table_id}` "
        f"WHERE {where_clause} LIMIT 1"
    )

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(query, job_config=job_config)

    try:
        results = list(query_job)
        if results:
            logging.info("Skipping batch of records as match already exists.")
            return
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Error querying existing records: %s", e)
        return

    # Proceed with loading the records if no matching dimensions were found
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,  # Pass the schema to the job configuration
    )

    try:
        load_job = client.load_table_from_json(
            json.loads(json.dumps(records)), table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        logging.info("Loaded batch of records.")
    except ClientError as err:
        logging.error("load_data_to_bigquery(): %s", load_job.errors)
        raise err
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Failed to load batch of records: %s", e)


def load_dataframe_to_bigquery_truncate(
    client, dataset_id, table_id, dataframe
):
    """
    Load a pandas DataFrame into a BigQuery table. truncate method

    Parameters:
    - client: BigQuery Client object
    - dataset_id: Dataset ID where the table resides
    - table_id: Table ID into which data is to be loaded
    - dataframe: DataFrame representing the records to be loaded
    - schema: autodetect
    """

    # Configuration for loading data into BigQuery
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        load_job = client.load_table_from_dataframe(
            dataframe, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        logging.info(
            "Loaded %s records to table %s.%s",
            len(dataframe),
            dataset_id,
            table_id,
        )
    except ClientError as err:
        logging.error(
            "load_dataframe_to_bigquery_truncate(): %s", load_job.errors
        )
        raise err
    except Exception as err:  # pylint: disable=broad-except
        logging.error(
            "Failed to load records to table: %s error: %s", table_id, str(err)
        )
        raise


def load_data_to_bigquery_truncate(
    client, dataset_id, table_id, records, schema
):
    """
    Load a list of dictionaries into a BigQuery table. truncate method

    Parameters:
    - client: BigQuery Client object
    - dataset_id: Dataset ID where the table resides
    - table_id: Table ID into which data is to be loaded
    - records: List of dictionaries representing the records to be loaded
    - schema: BigQuery schema representing the table's structure
    """

    if not records:
        logging.info("No records to load.")
        return
    # Configuration for loading data into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        load_job = client.load_table_from_json(
            records, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        logging.info(
            "Loaded %s records to table %s.%s",
            len(records),
            dataset_id,
            table_id,
        )
    except ClientError as err:
        logging.error("load_data_to_bigquery_truncate(): %s", load_job.errors)
        raise err
    except Exception as err:  # pylint: disable=broad-except
        logging.error(
            "Failed to load records to temporary table: %s", str(err)
        )
        raise


def load_data_to_bigquery_append(
    client, dataset_id, table_id, records, schema
):
    """
    Load a list of dictionaries into a BigQuery table. append method

    Parameters:
    - client: BigQuery Client object
    - dataset_id: Dataset ID where the table resides
    - table_id: Table ID into which data is to be loaded
    - records: List of dictionaries representing the records to be loaded
    - schema: BigQuery schema representing the table's structure
    """
    if not records:
        logging.info("No records to load.")
        return

    # Prepare the BigQuery job config
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    logging.info(
        "Loading records to BigQuery table: %s.%s", dataset_id, table_id
    )
    table_ref = client.dataset(dataset_id).table(table_id)
    logging.info("Table reference: %s", table_ref)
    try:
        # Serialize records to newline-delimited JSON strings
        # Load the JSON strings to BigQuery
        load_job = client.load_table_from_json(
            records, table_ref, job_config=job_config
        )
        load_job.result()  # Wait for the job to complete
        logging.info(
            "Loaded %s records to %s.%s", len(records), dataset_id, table_id
        )
    except ClientError as err:
        logging.error("load_data_to_bigquery_append(): %s", load_job.errors)
        raise err
    except Exception as err:  # pylint: disable=broad-except
        logging.error("Failed to load records: %s", str(err))
        raise


def merge_temp_table_to_target(
    client, target_table, temp_table, schema, primary_keys
):
    """Merge temp table to target table"""
    merge_sql = generate_bq_merge_statement(
        target_table, temp_table, schema, primary_keys
    )
    # logging.info("merge_sql: %s", merge_sql)
    run_bq_sql(client, merge_sql)


def load_csv_to_bigquery(client, dataset_id, csv_file_path):
    """
    Loads a CSV file into BigQuery with schema inferred from the first row of the CSV file.

    Args:
        client (bigquery.Client): The BigQuery client.
        dataset_id (str): The BigQuery dataset ID.
        csv_file_path (str): The path to the CSV file to load into BigQuery.
    """
    try:
        # Replace spaces with underscores in the CSV file path to ensure proper access
        csv_file_path = csv_file_path.replace(" ", "_")

        # Generate a table ID from the CSV file name
        table_id = os.path.splitext(os.path.basename(csv_file_path))[0]
        table_id = "_".join(table_id.split("_")[:-1])
        table_id = table_id.lower()

        # Read the CSV file to infer schema
        df = pd.read_csv(
            csv_file_path, nrows=0
        )  # Read only the header row to infer column names
        schema = [
            bigquery.SchemaField(column, "STRING") for column in df.columns
        ]  # Treat all columns as STRING

        # Define the full table ID
        full_table_id = f"{dataset_id}.{table_id}"

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip the header row
            autodetect=False,  # Schema is being manually provided
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        # Load the CSV file into BigQuery
        with open(csv_file_path, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, full_table_id, job_config=job_config
            )

        # Wait for the load job to complete
        load_job.result()
        print(f"Loaded {csv_file_path} into {full_table_id}")
        os.remove(csv_file_path)
        print(f"Deleted CSV file: {csv_file_path}")

    except Exception as e:
        print(f"Error loading {csv_file_path} to BigQuery: {e}")
