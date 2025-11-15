''' functions for compare delete bigq and sqlserver '''
import logging
# import json
import sqlalchemy
from pandas_gbq import to_gbq
from google.cloud import bigquery

def data_deletion_ids_to_bigquery(dataframe, temp_table_id_bigquery, google_cred):
    """
    Write data deletion DataFrame to Bigquery CDW
    """
    logging.info("data_deletion_ids_to_bigquery(): destination_table: %s", temp_table_id_bigquery)
    to_gbq(
        dataframe,
        destination_table=temp_table_id_bigquery,
        project_id='hexa-data-report-etl-prod',
        credentials=google_cred,
        if_exists='replace'
        )

def data_deletion_ids_to_sqlserver(dataframe, conn):
    """
    Write data deletion DataFrame to SQL Server CDW
    """
    engine = sqlalchemy.create_engine(conn, fast_executemany=True)
    dataframe.to_sql(
        'data_deletion',
        engine,
        schema='temp',
        if_exists='replace',
        dtype={
            'email': sqlalchemy.types.String(),
            'user__id': sqlalchemy.types.Integer(),
            'marketing_id_email': sqlalchemy.types.String(),
            'marketing_id': sqlalchemy.types.String(),
        },
        chunksize=50000
    )

def drop_table_sqlserver(conn):
    """
    Drop the data_deletion table from SQL Server.
    """
    engine = sqlalchemy.create_engine(conn, fast_executemany=True)
    with engine.connect() as connection:
        connection.execute("DROP TABLE IF EXISTS temp.data_deletion;")
    logging.info("SQL Server: data_deletion table dropped.")

def drop_table_bigquery(google_cred, dataset_id, table_id):
    """
    Drop the data_deletion table from Google BigQuery.
    """
    client = bigquery.Client(credentials=google_cred)
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    try:
        client.delete_table(table_ref)
        logging.info("BigQuery: %s table dropped.", table_ref)
    except Exception as e:
        logging.error("BigQuery: Error dropping %s - %s", table_ref, e)

def compare_and_delete_bigquery(temp_table_name, source_tables_with_keys, credentials_path):
    """
    compare bigquery table(s) to data deletion and run DELETE stmt for matches
    """
    function_name = 'compare_and_delete_bigquery'
    credentials = credentials_path
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Fetch subs_ids from matrix__customer
    subs_ids_query = f"""
        SELECT 
        distinct subscriber.subs_id as subs_id
        FROM `hexa-data-report-etl-prod.cdw_stage_matrix.tbl_person` tbl_person
        inner join `hexa-data-report-etl-prod.cdw_stage_matrix.subscriber` subscriber
        on (subscriber.subs_perid = tbl_person.person_pointer)
        INNER JOIN `{temp_table_name}` dd
        ON (dd.email IS NOT NULL AND tbl_person.PersContact3 = dd.email)
    """
    subs_ids_result = client.query(subs_ids_query).result()
    subs_ids = [row.subs_id for row in subs_ids_result]

    for table_config in source_tables_with_keys:
        existing_table_name = table_config['table_name']
        try:
            logging.info(
                "[%s]: Processing deletion for table: %s",
                function_name,
                existing_table_name
                )

            if table_config.get('requires_subs_id', False):
                match_field = table_config.get('match_field', 'subs_id') # Defaulting to 'subs_id' if not specified
                delete_query = f"""
                    DELETE FROM `{existing_table_name}`
                    WHERE CAST(`{match_field}` AS INT64) IN UNNEST(@subs_ids)
                """
                query_params = [bigquery.ArrayQueryParameter('subs_ids', 'INT64', subs_ids)]
                job_config = bigquery.QueryJobConfig(query_parameters=query_params)

            else:
                    # Construct clauses for matching conditions
                    match_conditions = []
                    columns_to_check = ['user_id',
                                        'uid',
                                        'user__uid',
                                        '`user`.`uid`',
                                        'User_ID__UID_'
                                        ]
                    for temp_col, existing_col in table_config.get('match_keys', {}).items():
                        not_null_check = f"data_deletion.{temp_col} IS NOT NULL"
                        if 'email' in [temp_col, existing_col]:
                            condition = (
                                f"LOWER(TRIM(`{existing_table_name}`.{existing_col})) IN "
                                f"(SELECT LOWER(TRIM({temp_col})) FROM `{temp_table_name}` data_deletion WHERE {not_null_check})"
                            )
                        elif any(col in [temp_col, existing_col] for col in columns_to_check):
                            condition = (
                                f"SAFE_CAST(`{existing_table_name}`.{existing_col} AS INT64) IN "
                                f"(SELECT SAFE_CAST({temp_col} AS INT64) FROM `{temp_table_name}` data_deletion WHERE {not_null_check})"
                            )
                        else:
                            condition = (
                                f"`{existing_table_name}`.{existing_col} IN "
                                f"(SELECT {temp_col} FROM `{temp_table_name}` data_deletion WHERE {not_null_check})"
                            )
                        match_conditions.append(condition)
                    on_clause = " OR ".join(match_conditions)

                    delete_query = f"""
                        DELETE FROM `{existing_table_name}`
                        WHERE {on_clause}
                    """
                    job_config = None

            # Execute the delete query
            query_job = client.query(delete_query, job_config=job_config)
            query_job.result()

            # Log successful deletion
            logging.info(
                "[%s]: Delete operation performed on %s. Rows affected: %s",
                function_name,
                existing_table_name,
                query_job.num_dml_affected_rows
                )

        except Exception as biqqerror:
            logging.error(
                "[%s]: Error processing table %s: %s",
                function_name,
                existing_table_name,
                biqqerror
            )
            raise

def compare_and_delete_sqlserver(temp_table_name, source_tables_with_keys, conn):
    """
    sqlserver prepare delete
    """
    function_name = 'compare_and_delete_sqlserver'
    engine = sqlalchemy.create_engine(conn, fast_executemany=True)

    with engine.connect() as connection:
        # Fetch subs_ids and ObjectPointers if needed
        subs_ids = []
        object_pointers = []
        for table_config in source_tables_with_keys:
            if table_config.get('requires_subs_ids', False):
                # Fetch subs_ids from the customer table
                fetch_subs_ids_query = f"""
                    SELECT DISTINCT subs_id
                    FROM [stage].[matrix].[customer] cust
                    INNER JOIN {temp_table_name} dd
                    ON (dd.email IS NOT NULL AND LOWER(TRIM(cust.PersContact3)) = LOWER(TRIM(dd.email)))
                    OR (dd.marketing_id IS NOT NULL AND cust.marketing_id = dd.marketing_id)
                    OR (dd.marketing_id_email IS NOT NULL AND cust.marketing_id = dd.marketing_id_email)
                """
                result = connection.execute(fetch_subs_ids_query)
                subs_ids.extend([row.subs_id for row in result])

            elif table_config.get('requires_objectpointer_from_person', False):
                # Fetch ObjectPointers from the person table
                fetch_object_pointers_query = f"""
                    SELECT DISTINCT ObjectPointer
                    FROM [stage].[matrix].[tbl_person] person
                    INNER JOIN {temp_table_name} dd
                    ON LOWER(TRIM(person.PersContact3)) = LOWER(TRIM(dd.email))
                """
                result = connection.execute(fetch_object_pointers_query)
                object_pointers.extend([row.ObjectPointer for row in result])

        # Delete from tables
        for table_config in source_tables_with_keys:
            existing_table_name = table_config['table_name']
            try:
                if table_config.get('requires_subs_ids', False):
                    # Determine the correct column name to use for deletion
                    column_name = table_config['match_keys']['subs_id']
                    # Delete using subs_ids for matching
                    delete_query = f"""
                        DELETE FROM {table_config['table_name']}
                        WHERE {column_name} IN ({','.join(['?']*len(subs_ids))})
                    """
                    result = connection.execute(delete_query, subs_ids)

                elif table_config.get('requires_objectpointer_from_person', False):
                    # Delete using object_pointers for matching
                    delete_query = f"""
                        DELETE FROM {table_config['table_name']}
                        WHERE ObjectPointer IN ({','.join(['?']*len(object_pointers))})
                    """
                    result = connection.execute(delete_query, object_pointers)

                else:
                    # Construct clauses for matching conditions, including transformations for email
                    match_conditions = []
                    for temp_col, existing_col in table_config['match_keys'].items():
                        not_null_check = f"data_deletion.{temp_col} IS NOT NULL"
                        if existing_table_name.lower() == "stage.piano.piano__subscriptions":
                            # Apply special handling for stage.piano.piano__subscriptions table
                            if 'email' in [temp_col, existing_col]:
                                condition = (
                                    f"({not_null_check} AND "
                                    f"LOWER(TRIM(CAST({existing_table_name}.{existing_col} AS VARCHAR))) = "
                                    f"LOWER(TRIM(CAST(data_deletion.{temp_col} AS VARCHAR))))"  # Ensure closing parentheses
                                )
                            else:
                                condition = (
                                    f"({not_null_check} AND "
                                    f"{existing_table_name}.{existing_col} = "
                                    f"CAST(data_deletion.{temp_col} AS VARCHAR))"  # Correctly close the CAST operation
                                )
                        else:
                            if 'email' in [temp_col, existing_col]:
                                condition = (
                                    f"({not_null_check} AND "
                                    f"LOWER(TRIM({table_config['table_name']}.{existing_col})) = "
                                    f"LOWER(TRIM(data_deletion.{temp_col})))"  # For non-special tables, handle email similarly without CAST
                                )
                            else:
                                condition = (
                                    f"({not_null_check} AND "
                                    f"{table_config['table_name']}.{existing_col} = "
                                    f"data_deletion.{temp_col})"
                                )

                        match_conditions.append(condition)
                    on_clause = " OR ".join(match_conditions)

                    # Construct the delete query for direct match with data_deletion
                    delete_query = f"""
                        DELETE FROM {table_config['table_name']}
                        WHERE EXISTS (
                            SELECT 1 FROM {temp_table_name} data_deletion
                            WHERE {on_clause}
                        )
                    """
                    result = connection.execute(delete_query)

                rows_deleted = result.rowcount
                logging.info(
                    "[%s]: Deleted %s rows from %s",
                    function_name,
                    rows_deleted,
                    table_config['table_name']
                    )

            except Exception as error:
                logging.info(
                    "[%s]: Error deleting from %s: %s",
                    function_name,
                    table_config['table_name'],
                    error
                    )
                raise

def delete_from_vault_bigquery(config_file, credentials_path):
    """
    delete from the bigquery vault
    """
    # Load configuration
    function_name = 'delete_from_vault_bigquery'
    config = config_file

    # Create BigQuery client
    credentials = credentials_path
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Build the staging query with join conditions
    staging_config = config["staging_table"]
    join_config = config["join_conditions"]
    data_deletion_table = config["data_deletion_table"]
    staging_query = f"""
        SELECT {', '.join(staging_config["fields_to_retrieve"])}
        FROM `{staging_config["name"]}`
        INNER JOIN `{data_deletion_table}` ON CAST(`{staging_config["name"]}`.{join_config["staging_table_field"]} AS INT64) = CAST(`{data_deletion_table}`.{join_config["data_deletion_field"]} AS INT64)
        AND `{data_deletion_table}`.{join_config["data_deletion_field"]} IS NOT NULL
        WHERE {staging_config["condition"]}
    """
    staging_result = client.query(staging_query).result()
    response_ids = [row[staging_config["fields_to_retrieve"][0]] for row in staging_result]

    # Fetch RESPONSE_HASH from the satellite table
    satellite_config = config["satellite_table"]
    hash_query = f"""
        SELECT DISTINCT {satellite_config["hash_field"]}
        FROM `{satellite_config["name"]}`
        WHERE {satellite_config["join_field"]} IN UNNEST(@response_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("response_ids", "STRING", response_ids)
        ]
    )
    hash_result = client.query(hash_query, job_config=job_config).result()
    response_hashes = [row[satellite_config["hash_field"]] for row in hash_result]

    # Delete from tables in the specified order
    for table_config in config["deletion_order"]:
        try:
            table_name = table_config["table_name"]
            hash_column = table_config["hash_column"]
            delete_query = f"""
                DELETE FROM `{table_name}`
                WHERE {hash_column} IN UNNEST(@response_hashes)
            """
            delete_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter("response_hashes", "STRING", response_hashes)
                ]
            )

            query_job = client.query(delete_query, delete_job_config)
            query_job.result()  # Wait for the job to complete

            # Log the number of rows deleted
            logging.info(
                "[%s]: Delete operation performed on %s. Rows affected: %s",
                function_name,
                table_name,
                query_job.num_dml_affected_rows
                )

        except Exception as error:
            logging.info("[%s]: Error processing table %s: %s", function_name, table_name, error)
            raise

    # Final step: Delete matching records from the staging table
    # Final step: Delete directly from the staging table based on data_deletion matches
    try:
        final_delete_query = f"""
            DELETE FROM `{staging_config["name"]}`
            WHERE CAST(`{staging_config["name"]}`.{join_config["staging_table_field"]} AS INT64) IN (
                SELECT DISTINCT CAST(`{data_deletion_table}`.{join_config["data_deletion_field"]} AS INT64)
                FROM `{data_deletion_table}`
                WHERE `{data_deletion_table}`.{join_config["data_deletion_field"]} IS NOT NULL
            AND {staging_config["condition"]}
            )
        """

        final_query_job = client.query(final_delete_query)
        final_query_job.result()  # Wait for the job to complete

        # Log the number of rows deleted from the staging table
        logging.info(
            "[%s]: Final delete operation performed on %s. Rows affected: %s",
            function_name,
            staging_config['name'],
            final_query_job.num_dml_affected_rows
            )

    except Exception as error:
        logging.info(
            "[%s]: Error in final deletion from staging table %s: %s",
            function_name,
            staging_config['name'],
            error
            )
        raise

def delete_from_sql_vault(config_file, connection_string):
    """
    delete from the sqlserver vault matches to data_deletion table
    """
    function_name = 'delete_from_sql_vault'
    config = config_file['sql_server']

    # Create SQLAlchemy engine
    engine = sqlalchemy.create_engine(connection_string, fast_executemany=True)

    # Fetch customer hashes to delete
    staging_table = config['staging_table']
    data_deletion_table = config['data_deletion_table']
    match_query = f"""
        SELECT DISTINCT hash_key
        FROM {staging_table} st
        INNER JOIN {data_deletion_table} dd
        ON (dd.email IS NOT NULL AND LOWER(TRIM(st.PersContact3)) = LOWER(TRIM(dd.email)))
        OR (dd.marketing_id IS NOT NULL AND st.marketing_id = dd.marketing_id )
        OR (dd.marketing_id_email IS NOT NULL AND st.marketing_id = dd.marketing_id_email )
    """

    with engine.connect() as connection:
        result = connection.execute(match_query)
        customer_hashes = [row.hash_key for row in result]

        # Get hashes from link tables
        cancellation_hashes, subscription_hashes = [], []
        for customer_hash in customer_hashes:
            link_cancellation_query = f"""
                SELECT cancellation_hash
                FROM [vault].[raw].[link_cancellation]
                WHERE customer_hash = '{customer_hash}' AND record_source = 'MATRIX'
            """
            cancellation_hashes.extend(
                [row.cancellation_hash for row in connection.execute(link_cancellation_query)]
                )

            link_subscription_query = f"""
                SELECT subscription_hash
                FROM [vault].[raw].[link_subscription]
                WHERE customer_hash = '{customer_hash}' AND record_source = 'MATRIX'
            """
            subscription_hashes.extend(
                [row.subscription_hash for row in connection.execute(link_subscription_query)]
                )

        # Delete from tables in the specified order
        for table_config in config['deletion_order']:
            table_name = table_config['table_name']
            if 'sat_cancellation' in table_name:
                ref_column = 'cancellation_hash'
                hashes = cancellation_hashes
                logging.info("[%s]: sat_cancellation %s rows to delete", function_name, len(hashes))
            elif 'sat_subscription' in table_name:
                ref_column = 'subscription_hash'
                hashes = subscription_hashes
                logging.info("[%s]: sat_subscription %s rows to delete", function_name, len(hashes))
            else:
                ref_column = table_config['reference_column']
                hashes = customer_hashes

            condition_column = table_config['condition_column']
            condition_value = table_config['condition_value']

            if hashes:
                # Build and execute the DELETE query
                delete_query = f"""
                    DELETE FROM {table_name}
                    WHERE {ref_column} IN ({','.join(['?']*len(hashes))})
                    AND {condition_column} = ?
                """
                try:
                    result = connection.execute(delete_query, *hashes, condition_value)
                    logging.info(
                        "[%s]: Deleted from %s: %s rows affected.",
                        function_name,
                        table_name,
                        result.rowcount
                        )
                except Exception as error:
                    logging.info(
                        "[%s]: Error deleting from %s: %s",
                        function_name,
                        table_name,
                        error
                        )
                    # SQLAlchemy automatically rolls back the transaction on error
                    raise
