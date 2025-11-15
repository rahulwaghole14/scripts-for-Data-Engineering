# Qualtrics Data Processing

## Overview
This project contains scripts to process and export survey data from Qualtrics to Google BigQuery. The data is extracted from Qualtrics, and loaded into BigQuery tables.

## Prerequisites
Please install the requirements.txt before running. 
`pip install -r requirements.txt` 

## Key Scripts

### `qualtrics_data_process.py`

This script contains the main functions for processing and exporting Qualtrics data:

- `initialize_config(survey_id, survey_name)`: Sets up the configuration using environment variables and AWS Secrets Manager.
- `request_export(export_start_date, export_end_date, export_config)`: Requests data export from Qualtrics.
- `check_progress(check_progress_id, check_config)`: Checks the progress of the data export.
- `download_and_read_zip(dl_file_id, download_config)`: Downloads and reads the exported data from Qualtrics.
- `create_bigquery_table_if_not_exists(client, project_id, dataset_id, table_id, schema)`: Creates a BigQuery table if it doesn't exist.
- `load_into_bigquery(bq_df, bq_config, google_cred)`: Loads data into BigQuery.
- `qualtrics_run(survey_id, survey_name, start_date_str, end_date_str)`: Main function to run the data extraction, transformation, and loading process.

### `qualtrics__news_hub.py` and `qualtrics__reader_feedback.py`

This script runs the `qualtrics_run` function for the `NEWS_HUB_SURVEY`:

- Retrieves the survey ID and API credentials from AWS Secrets Manager.
- Sets the date range for the data export.
- Converts the date range from UTC to New Zealand Time (NZT).
- Calls the `qualtrics_run` function to process and export the data.

## Future use
For future use, if there is a new survey that needs to be extracted to BigQuery, you can follow the approach used in qualtrics__news_hub.py. Just update the script with the new survey ID and survey name:

- Create a new script similar to qualtrics__news_hub.py.

- Update the survey ID and survey name variables to reflect the new survey.
