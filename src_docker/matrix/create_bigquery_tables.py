import pyodbc
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict
from google.cloud.exceptions import NotFound
import logging
from .load_config import load_config
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO)


def create_bigquery_client():
    config = load_config()
    google_cred = config["google_cred"]
    try:
        credentials = service_account.Credentials.from_service_account_info(
            google_cred,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )
        logging.info("Created BigQuery client")
    except Exception as error:
        logging.error(f"Failed to create BigQuery client: {error}")
        client = None
    return client


def map_data_types(sql_server_type):
    mapping = {
        "int": "INT64",
        "bigint": "INT64",
        "tinyint": "INT64",
        "smallint": "INT64",
        "varchar": "STRING",
        "char": "STRING",
        "datetime": "TIMESTAMP",
        "smalldatetime": "DATETIME",
        "date": "DATE",
        "bit": "BOOL",
        "float": "FLOAT64",
        "numeric": "NUMERIC",
        "decimal": "NUMERIC",  # Use NUMERIC for decimal to maintain precision
    }
    return mapping.get(
        sql_server_type.lower(), "STRING"
    )  # Default to STRING if type unknown


def extract_schema_from_sql_server(
    server,
    portnumber,
    database,
    username,
    password,
    tables_to_extract,
    columns_to_include,
):
    conn_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server},{portnumber};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
    try:
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        schema_info = {}
        for schema_name, table_name in tables_to_extract:
            # Form the full table name for use in BigQuery table creation, including schema only if not 'dbo'
            full_table_name = (
                f"{schema_name}.{table_name}"
                if schema_name != "dbo"
                else table_name
            )

            query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema_name}'
            """
            cursor.execute(query)
            columns = cursor.fetchall()
            if table_name in columns_to_include:
                # Filter columns for this table based on the predefined list
                filtered_columns = [
                    col
                    for col in columns
                    if col[0] in columns_to_include[table_name]
                ]
                schema_info[full_table_name] = [
                    (col[0], map_data_types(col[1]))
                    for col in filtered_columns
                ]
            else:
                schema_info[full_table_name] = [
                    (col[0], map_data_types(col[1])) for col in columns
                ]
    except Exception as e:
        logging.error(f"Error extracting schema: {e}")
    finally:
        cursor.close()
        conn.close()
    return schema_info


def create_bigquery_table(
    client, dataset_name, full_table_name, schema_info, hash_key_tables
):
    try:
        bq_table_name = full_table_name.replace(".", "_")
        dataset_ref = client.dataset(dataset_name)
        table_ref = dataset_ref.table(bq_table_name)
        schema = [
            bigquery.SchemaField(column_name, data_type, mode="NULLABLE")
            for column_name, data_type in schema_info[full_table_name]
        ]
        # Add hash_key column to the schema for specified tables
        if full_table_name in hash_key_tables:
            schema.append(
                bigquery.SchemaField("hash_key", "STRING", mode="NULLABLE")
            )

        table = bigquery.Table(table_ref, schema=schema)
        try:
            created_table = client.create_table(table)
            logging.info(f"Created table {created_table.full_table_id}")
        except Conflict:
            logging.warning(
                f"Table {full_table_name} already exists in dataset {dataset_name}."
            )
    except Exception as e:
        logging.error(
            f"Error creating table {full_table_name} in dataset {dataset_name}: {e}"
        )


def create_bigquery_dataset(client, dataset_name, location):
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    try:
        client.get_dataset(dataset_id)
        logging.info(
            f"Dataset {dataset_name} already exists, skipping creation."
        )
    except NotFound:
        try:
            client.create_dataset(dataset)
            logging.info(
                f"Created dataset {client.project}.{dataset_name} in {location}"
            )
        except Exception as e:
            logging.error(f"Error creating dataset {dataset_name}: {e}")


def create_batch_metadata_table(client, dataset_name):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table("batch_metadata")
    schema = [
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("last_processed_pk", "INT64", mode="NULLABLE"),
        bigquery.SchemaField(
            "last_processed_date", "TIMESTAMP", mode="NULLABLE"
        ),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    try:
        created_table = client.create_table(table)
        logging.info(f"Created table {created_table.full_table_id}")
    except Conflict:
        logging.warning("Table batch_metadata already exists.")


def add_column_to_bigquery_table(client, dataset_name, full_table_name):
    """
    Adds an update_time column to the specified BigQuery table.
    """
    bq_table_name = full_table_name.replace(".", "_")
    table_id = f"{client.project}.{dataset_name}.{bq_table_name}"
    table = client.get_table(
        table_id
    )  # Make an API request to fetch the table

    # Check if the update_time column already exists to avoid duplication
    if not any(field.name == "update_time" for field in table.schema):
        new_schema = list(table.schema)  # Create a mutable copy of the schema
        new_schema.append(
            bigquery.SchemaField("update_time", "TIMESTAMP", mode="NULLABLE")
        )
        table.schema = new_schema  # Update the schema
        table = client.update_table(
            table, ["schema"]
        )  # Make an API request to update the table schema

        print(f"Added 'update_time' column to '{table_id}'.")
    else:
        print(f"'update_time' column already exists in '{table_id}'.")


def main():
    server = os.getenv("MATRIX_DB_SERVER")
    database = os.getenv("MATRIX_DB_DATABASE")
    username = os.getenv("MATRIX_DB_USERNAME")
    password = os.getenv("MATRIX_DB_PASSWORD")
    portnumber = os.getenv("MATRIX_DB_PORT")

    tables_to_extract = [
        ("dbo", "tbl_country"),
        ("dbo", "tbl_service"),
        ("dbo", "tbl_product"),
        ("dbo", "tbl_period"),
        ("dbo", "cancellation_reason"),
        ("dbo", "tbl_person"),
        ("dbo", "tbl_ContactNumber"),
        ("dbo", "tbl_location"),
        ("dbo", "subscriber"),
        ("dbo", "subscription"),
        ("dbo", "subord_cancel"),
        ("dbo", "complaints"),
        ("ffx", "temp_winback_subscribers1"),
        ("dbo", "rate_header"),
        ("dbo", "tbl_RateStructure"),
        ("dbo", "tbl_RateStructureItem"),
        ("dbo", "tbl_RateItem"),
        ("dbo", "TBL_LINKType"),
        ("dbo", "tbl_ObjectLink"),
        ("ffx", "tbl_RPT_TM1retail"),
        ("ffx", "vw_RPT_SubsRateRounds"),
    ]

    # Define tables that require a hash key column
    hash_key_tables = [
        "complaints",
        "subscriber",
        "subscription",
        "tbl_person",
        "tbl_ContactNumber",
        "subord_cancel",
        "tbl_location",
        "tbl_ObjectLink",
        "tbl_RPT_TM1retail",
        "vw_RPT_SubsRateRounds",
    ]
    columns_to_include = {
        "tbl_ObjectLink": [
            "Level1Pointer",
            "LinkObject",
            "LinkType",
            "ToDate",
            "ObjectLinkPointer",
        ]
    }

    client = create_bigquery_client()
    if client is None:
        logging.error("Failed to create BigQuery client.")
        return

    dataset_name = "cdw_stage_matrix"
    dataset_location = os.getenv("BIGQUERY_LOCATION")
    create_bigquery_dataset(client, dataset_name, dataset_location)
    create_batch_metadata_table(client, dataset_name)

    hash_key_tables = [
        f"{schema}.{table}" if schema != "dbo" else table
        for schema, table in tables_to_extract
        if table in hash_key_tables
    ]

    schema_info = extract_schema_from_sql_server(
        server,
        portnumber,
        database,
        username,
        password,
        tables_to_extract,
        columns_to_include,
    )
    for schema, table in tables_to_extract:
        full_table_name = f"{schema}.{table}" if schema != "dbo" else table
        if full_table_name in schema_info:
            create_bigquery_table(
                client,
                dataset_name,
                full_table_name,
                schema_info,
                hash_key_tables,
            )
            add_column_to_bigquery_table(client, dataset_name, full_table_name)


if __name__ == "__main__":
    main()
