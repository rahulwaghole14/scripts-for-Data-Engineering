from google.cloud import bigquery
from common.paths import SECRET_PATH
import logging


def biq_query_upload(client, df, dataset_id, table_id, schema):
    dimension_table_id = dataset_id + "." + table_id
    # if check_bq_table(client, dataset_id, table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=True, schema=schema, source_format=bigquery.SourceFormat.CSV
    )
    # df = df.drop(['Bounce Rate'], axis=1)

    df.columns = df.columns.str.replace("(", "", regex=False)
    df.columns = df.columns.str.replace(".", " dot ", regex=False)
    df.columns = df.columns.str.replace("-", "", regex=False)
    df.columns = df.columns.str.replace(")", "", regex=False)
    job = client.load_table_from_dataframe(
        df, dimension_table_id, job_config=job_config
    )
    output = job.result()
    logging.info(output)


def biq_query_upload_drop(client, df, dataset_id, table_id, schema):
    dimension_table_id = dataset_id + "." + table_id
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE",
    )
    df.columns = df.columns.str.replace("(", "", regex=False)
    df.columns = df.columns.str.replace(")", "", regex=False)
    job = client.load_table_from_dataframe(
        df, dimension_table_id, job_config=job_config
    )
    output = job.result()
    logging.info(output)


"""
steps:
determine BQ dataset
determine BQ table
check dates
if date not in dates: run script
- upload df to BQ


"""
