# Matrix Source to BigQuery Data Pipeline

## Overview
This project contains a set of Python scripts that facilitate the migration of data from a Matrix SQL server to Google BigQuery. It includes functionalities such as creating BigQuery tables, load and update data to the bigquery tables, data validation, and more.


## Prerequisites
Please install the requirements.txt before running.
`pip install -r requirements.txt`

## Key Scripts
### `create_bigquery_tables.py`
Runs the SQL queries necessary to create the BigQuery tables with the appropriate schema. This script only needs to run at the first time of the data pipeline.

### `load_data_to_bigquery_update.py`
Manages the transfer of data from the Matrix SQL server to BigQuery tables. Due to unreliable timestamps from the source table, data uploading and updating rely on a complete table comparison using generated hash keys for every data processing cycle.

### `data_comparison_and_clean_up.py`
Performs data comparison between the source and the destination and executes clean-up operations to ensure data integrity. This step is crucial as it also involves deleting any source data lines from the destination that have been removed from the source.

### `model_generator.py`
Generates Pydantic models for data validation prior to processing the data.
