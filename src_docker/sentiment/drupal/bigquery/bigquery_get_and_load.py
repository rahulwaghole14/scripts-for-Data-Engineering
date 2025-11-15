""" bigquery functions """
import logging
import traceback
from google.cloud import bigquery


def update_table_schema_with_new_fields(
    client, dataset_id, table_id, desired_schema
):
    """Update the BigQuery table schema to add new fields from the desired schema."""
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    table = client.get_table(
        table_ref
    )  # Make an API request to fetch the table

    # Generate dictionaries of field names to SchemaField objects for easy lookup
    current_schema_dict = {field.name: field for field in table.schema}
    desired_schema_dict = {field.name: field for field in desired_schema}

    # Identify new fields to add and existing fields for which data type might have changed
    new_fields = set(desired_schema_dict) - set(current_schema_dict)
    updated_schema = list(table.schema)  # Start with current schema

    # Add new fields to the schema
    for new_field_name in new_fields:
        updated_schema.append(desired_schema_dict[new_field_name])
        logging.info("Adding new field to schema: %s", new_field_name)
    # Update the table with the new schema
    table.schema = updated_schema
    client.update_table(table, ["schema"])
    logging.info("Schema updated successfully.")


def load_data(client, df, table_ref, schema):
    """Load dataframe to the table in BigQuery."""
    try:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result()
        logging.info("Job '%s' finished with state: %s", job.job_id, job.state)

        if job.state == "DONE" and not job.errors:
            logging.info("Data loaded successfully into BigQuery.")
        elif job.errors:
            logging.error("Job %s errors: %s", job.job_id, job.errors)
            for error in job.errors:
                logging.error(
                    "Error: %s, %s, %s",
                    error["location"],
                    error["message"],
                    error["reason"],
                )
        else:
            logging.warning(
                "Job %s completed but the state is not DONE: %s",
                job.job_id,
                job.state,
            )
    except Exception as error:  # pylint: disable=broad-except
        logging.error("Exception occurred in load_data():")
        logging.error(str(error))
        # Log the full traceback to help diagnose the issue
        traceback_str = traceback.format_exc()
        logging.error(traceback_str)


def delete_existing_rows_by_id(client, dataset_id, table_id, ids):
    """Delete rows in the BigQuery table with IDs matching those in the new data."""
    if not ids:
        logging.info("No IDs provided for deletion. Skipping delete step.")
        return

    ids_str = ",".join([f"'{id}'" for id in ids])  # Format IDs for the query
    query = f"""
    DELETE FROM `{dataset_id}.{table_id}`
    WHERE id IN ({ids_str})
    """
    try:
        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete
        logging.info("Deleted existing rows from BigQuery table. %s", ids_str)
    except Exception as error:  # pylint: disable=broad-except
        logging.error("Error in delete_existing_rows_by_id(): %s", error)


def load_data_to_bigquery(client, df, dataset_id, table_id, schema):
    """parent function to run the bigquery flow"""
    try:
        table_ref = client.dataset(dataset_id).table(table_id)

        # Load data
        load_data(client, df, table_ref, schema)
    except Exception as e:  # pylint: disable=broad-except
        logging.error(
            "Exception occurred during load_data_to_bigquery() BigQuery operation: %s",
            e,
        )
