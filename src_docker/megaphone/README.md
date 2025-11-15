# Megaphone Data to BigQuery Data Pipeline

## Overview
This project provides a data pipeline for loading Megaphone data from an S3 bucket to Google BigQuery. It includes scripts for daily updates and backfilling data for specific dates.

## Prerequisites
Please install the requirements.txt before running. 
`pip install -r requirements.txt` 

## Key Scripts
### `megaphone_to_bigquery.py`
This script is designed to run daily at 6 PM to update the data from the previous day. According to the Megaphone team, from 00:00Z to 04:30Z, both the previous day's and current day's files are written once per hour. By 04:30Z each day, the previous day's file is final and will not be overwritten again.

### `megaphone_to_bigquery_backfill.py`
This script can be used to load data for selected dates, allowing for backfilling of data.

### `validator.py`
This script contains Pydantic models (Impression and Metrics) to validate data before loading it into BigQuery.


For more information regarding megaphone data, please refer to [metrics-export-service](https://intercom.help/megaphone/en/articles/2678649-metrics-export-service)