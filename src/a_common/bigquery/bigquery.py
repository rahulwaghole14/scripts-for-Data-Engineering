""" some helpful functions for bigquery """

import json
import base64
import logging
from datetime import date, datetime, time
from typing import List, get_origin, get_args, Union, Type
import pandas as pd
from pydantic import BaseModel
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
from google.api_core.exceptions import ClientError

from a_common.validation.validators import MyBaseModel


class UnsupportedTypeError(Exception):
    """hello"""


def is_pydantic_model(some_type: Type) -> bool:
    """
    Check if the given type is a subclass of MyBaseModel.
    """
    return isinstance(some_type, type) and issubclass(some_type, MyBaseModel)


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
        logging.error(query_job.errors)
        raise err
    except Exception as err:
        logging.error("SQL query operation failed: %s", str(err))
        raise


def run_bq_sql(client, sql):
    """run sql command in bq"""
    try:
        query_job = client.query(sql)
        query_job.result()  # Wait for the query to complete
        logging.info("Sql query completed successfully.")
    except ClientError as err:
        logging.error(query_job.errors)
        raise err
    except Exception as err:
        logging.error("Sql query operation failed: %s", str(err))
        raise


def generate_bq_merge_statement(
    target_table, temp_table, schema, primary_keys
):
    """
    Generate a SQL MERGE statement to merge data from a temporary table into the target table.

    Args:
    - target_table (str): The fully-qualified BigQuery target table name.
    - temp_table (str): The fully-qualified BigQuery temporary table name.
    - schema (List[bigquery.SchemaField]): The BigQuery schema list.
    - primary_keys (List[str]): A list of field names that constitute the primary key.

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
    WHEN NOT MATCHED BY TARGET THEN INSERT ({insert_fields}) VALUES ({insert_values});
    """

    return merge_sql


def map_basic_type_to_bq_field(
    field_name: str, field_type: type, mode: str = "NULLABLE"
) -> bigquery.SchemaField:
    """
    Map Python basic types to BigQuery field types and create a SchemaField.

    Args:
    - field_name (str): The name of the field.
    - field_type (type): The Python type of the field.
    - mode (str): The mode of the field in BigQuery ('NULLABLE', 'REQUIRED', 'REPEATED').

    Returns:
    - bigquery.SchemaField: A BigQuery SchemaField corresponding to the Python type.
    """
    # Define a mapping from Python types to BigQuery types
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
) -> List[bigquery.SchemaField]:
    """
    Recursively create BigQuery schema from a Pydantic model, ensuring complex fields
    are appropriately marked as RECORD types.
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
                # If the items in the list are Pydantic models, handle as REPEATED RECORD
                nested_fields = pydantic_model_to_bq_schema(item_type)
                field_schema = bigquery.SchemaField(
                    name=field_name,
                    field_type="RECORD",
                    fields=nested_fields,
                    mode="REPEATED",
                )
            else:
                # If the list contains simple types, decide how to handle
                # based on your data structure
                # This could be a STRING or another simple type, depending on your data
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
            field_schema = map_basic_type_to_bq_field(field_name, field_type)

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


def create_or_update_table_schema(
    client, dataset_id, table_id, desired_schema
):
    """Create a BigQuery table if it doesn't exist or update its schema by adding new fields."""
    table_ref = client.dataset(dataset_id).table(table_id)

    try:
        table = client.get_table(table_ref)  # Attempt to get the table
        current_schema = table.schema
        updated_schema = current_schema.copy()

        # Determine if there are new fields in the desired schema not in the current schema
        current_field_names = {field.name for field in current_schema}
        for field in desired_schema:
            if field.name not in current_field_names:
                logging.info(
                    "Adding new field to BigQuery table schema: %s", field.name
                )
                updated_schema.append(field)  # Add new field to schema

        # If the schema was updated, update the table's schema
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
    Load a list of dictionaries into a BigQuery table with dimension-based deduplication,
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

    # If any dimension set has more than one value, records have different dimensions
    if any(len(values) > 1 for values in dimension_values.values()):
        logging.error("Not all records have the same dimensions.")
        return

    # Convert date objects to ISO format strings
    for record in records:
        for key, value in record.items():
            if isinstance(value, date):
                record[key] = value.isoformat()

    # Since dimensions are the same for all records, use the first record for checks
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
            logging.info(
                "Skipping batch of records as matching dimensions already exist."
            )
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
        logging.error(load_job.errors)
        raise err
    except Exception as e:  # pylint: disable=broad-except
        logging.error("Failed to load batch of records: %s", e)


def load_dataframe_to_bigquery_truncate(
    client, dataset_id, table_id, dataframe, schema=None
):
    """
    Load a pandas DataFrame into a BigQuery table. truncate method

    Parameters:
    - client: BigQuery Client object
    - dataset_id: Dataset ID where the table resides
    - table_id: Table ID into which data is to be loaded
    - dataframe: DataFrame representing the records to be loaded
    - schema: Optional BigQuery schema, default is autodetect
    """

    if not isinstance(dataframe, pd.DataFrame):
        raise ValueError(
            "The 'dataframe' parameter must be a pandas DataFrame"
        )

    # Configuration for loading data into BigQuery
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True

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
        logging.error(load_job.errors)
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
        logging.error(load_job.errors)
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

    table_ref = client.dataset(dataset_id).table(table_id)

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
        logging.error(load_job.errors)
        raise err
    except Exception as err:  # pylint: disable=broad-except
        logging.error("Failed to load records: %s", str(err))
        raise
