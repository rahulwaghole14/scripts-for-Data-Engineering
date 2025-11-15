import logging
import os
from datetime import datetime
import requests
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig
from google.cloud.exceptions import GoogleCloudError
from dotenv import load_dotenv
from logger import logger

# Load environment variables
load_dotenv()
logger("naviga")

# Define the API URL
api_url = os.environ.get("api_naviga")


def fetch_and_save_csv(api_url):

    try:
        # Send a GET request to the API with timeout
        response = requests.get(api_url, timeout=500)

        # Check if the request was successful
        if response.status_code == 200:
            # Extract the CSV data from the response
            csv_data = response.content.decode("utf-8")

            # Specify the network drive directory
            NETWORK_DRIVE_PATH = r"\\pdcfilcl102\group$\FFX\Datateam\hexa_Naviga_Informer_report"
            # NETWORK_DRIVE_PATH = os.path.expanduser("~/Desktop")

            # Generate a timestamp
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Construct the file name with timestamp
            file_name = f"data_{timestamp}.csv"

            # Save the CSV data to a file on the network drive
            file_path = os.path.join(NETWORK_DRIVE_PATH, file_name)
            with open(
                file_path, "w", encoding="utf-8", newline=""
            ) as csv_file:
                csv_file.write(csv_data)

    except requests.exceptions.Timeout:
        logging.info("The request timed out.")
    except requests.exceptions.RequestException as e:
        logging.info("An error occurred: %s", e)

    return file_path


def upload_csv_to_bigquery(dataset_id, table_id, csv_path):
    try:
        client = bigquery.Client()

        dtype_mapping = {
            "Print Pub Ind": "str",
            "Advertiser Id": "int64",
            "Campaign ID": "str",
            "Line ID": "int64",
            "Advertiser Name": "str",
            "Month Actual Amt": "str",
            "Month Est Amt": "str",
            "Month Start Date": "str",
            "Month Unique ID": "float64",
            "Campaign Type": "str",
            "Campaign Status Code": "str",
            "Line Cancel Status ID": "str",
            "Currency Exchange Rate": "str",
            "Currency Code": "str",
            "Agency Commission Percent": "str",
            "No Agy Comm Ind": "str",
            "Actual Line Local Amount": "str",
            "Est Line Local Amount": "str",
            "Gross Line Local Amount": "str",
            "Net Line Local Amount": "str",
            "Gross Line Foreign Amount": "str",
            "Net Line Foreign Amount": "str",
            "Current Rep ID": "str",
            "Current Rep Pct": "str",
            "Current Rep Name": "str",
            "Net Rep Amount": "str",
            "Product ID": "str",
            "Product Name": "str",
            "Primary Group ID": "str",
            "Date Entered": "str",
            "Client Type ID": "str",
            "Ad Type ID": "str",
            "Issue Date": "str",
            "Product Grouping": "str",
            "Primary Rep Group ID": "str",
            "Primary Rep Group": "str",
            "Agency ID": "str",
            "Agency Name": "str",
            "Brand PIB Code": "str",
            "AD Internet Campaigns Brand Id": "str",
            "PIB Category Desc": "str",
            "GL Type ID": "str",
            "GL Types Description": "str",
            "Size Desc": "str",
            "Advertiser Legacy": "str",
            "Agency Legacy": "str",
            "AD Internet Sections Section Description": "str",
            "Orig Rep Report Ids": "str",
            "Curr Rep Report Ids": "str",
            "Orig Rep ID": "str",
            "Timestamp": "str",
            "Est Qty": "str",
            "Month Actual Imps": "str",
        }

        dataframe = pd.read_csv(csv_path, dtype=dtype_mapping)
        dataframe["Campaign ID"] = (
            dataframe["Campaign ID"].str.replace(",", "").astype(float)
        )

        date_cols = ["Month Start Date", "Date Entered", "Issue Date"]
        for col in date_cols:
            dataframe[col] = pd.to_datetime(dataframe[col])

        dataframe["Timestamp"] = pd.to_datetime(dataframe["Timestamp"])

        # Perform any necessary transformation on dataframe here

        job_config = LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",  # Choose the appropriate write mode based on your requirement
        )

        # Load the dataframe to BigQuery
        job = client.load_table_from_dataframe(
            dataframe, f"{dataset_id}.{table_id}", job_config=job_config
        )
        job.result()  # Wait for the job to complete

        logging.info(
            "Uploaded %s to %s.%s in BigQuery", csv_path, dataset_id, table_id
        )
    except GoogleCloudError as error:
        logging.error("Error uploading CSV to BigQuery: %s", error)


# Usage
api_url = os.environ.get("api_naviga")  # URL to fetch data
dataset_id = os.environ.get("dataset_id_naviga")
table_id_base = os.environ.get("table_id_naviga")
current_date_bq = datetime.now().strftime("%Y%m%d")
table_id = f"{table_id_base}_{current_date_bq}"

csv_file_path = fetch_and_save_csv(api_url)  # Fetch and save CSV
# csv_file_path = 'src/naviga__informer_reports_to_rdw/data_20231102062808.csv'

if csv_file_path:
    upload_csv_to_bigquery(
        dataset_id, table_id, csv_file_path
    )  # Upload to BigQuery
