# CSV Data Download and Upload to Google BigQuery

This Python script is designed to download CSV data from a specified API, save it to a network drive, and then upload it to Google BigQuery. It uses the `requests` library to make API requests, saves the CSV data to a file on a network drive, and uploads the data to BigQuery using the `upload_csv_to_bigquery` function. Please also refer to https://hexanz.atlassian.net/wiki/spaces/CD/pages/2810183711/Data+Lineage

## Program Overview

The program serves the following purposes:

- **Data Retrieval**: It fetches CSV data from a designated API endpoint. The API URL is specified as an environment variable (`api_naviga`).

- **Data Storage**: The retrieved CSV data is saved to a network drive directory. The directory path is configurable in the script.

- **Data Transformation**: If necessary, data can be transformed or processed before uploading it to BigQuery. The script currently assumes no specific data transformation.

- **Data Upload**: The script uploads the CSV data to Google BigQuery. It uses the BigQuery Python client library and relies on the `upload_csv_to_bigquery` function from the 'data_to_bigquery' module.

## Prerequisites

Before running the script, make sure you have the following prerequisites in place:

1. **Python Environment**: Ensure that you have a Python environment set up.

2. **Required Python Packages**: Install the necessary Python packages by running:

   ```bash
   pip install -r requirements.txt
