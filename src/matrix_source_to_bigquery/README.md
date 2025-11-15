# Matrix Source to BigQuery Data Pipeline

## Overview
This project contains a set of Python scripts that facilitate the migration of data from a Matrix SQL server to Google BigQuery. It includes functionalities such as creating BigQuery tables, load and update data to the bigquery tables, data validation, and more.


## Prerequisites
Please install the requirements.txt before running. 
`pip install -r requirements.txt` 

## Key Scripts
### `load_config.py`
Loads configuration settings include database connection strings, BigQuery project details, and other necessary parameters.

### `create_bigquery_tables.py`
Runs the SQL queries necessary to create the BigQuery tables with the appropriate schema. This script only needs to run at the first time of the data pipeline.

### `load_data_to_bigquery_update.py`
Manages the transfer of data from the Matrix SQL server to BigQuery tables. Due to unreliable timestamps from the source table, data uploading and updating rely on a complete table comparison using generated hash keys for every data processing cycle.

### `data_comparison_and_clean_up.py`
Performs data comparison between the source and the destination and executes clean-up operations to ensure data integrity. This step is crucial as it also involves deleting any source data lines from the destination that have been removed from the source.

### `model_generator.py`
Generates Pydantic models for data validation prior to processing the data.

### `main.bat`
A batch file that orchestrates the execution of the pipeline, typically used to schedule the pipeline to run at intervals.

### `logger.py`
Provides a logging mechanism to track the process of the data pipeline execution.



## Trouble shooting
The following scripts are useful for troubleshooting or testing issues within the data pipeline.

### `data_validation.py`
Validates the data to ensure that it meets the necessary conditions and formats before migration.

### `compare_keys.py`
Compares keys between the Matrix SQL server and the BigQuery tables to ensure consistency.

### `delete_truncate_table.py`
Deletes or truncates tables in BigQuery as part of the data refresh process.

### `test.py`
Contains tests to ensure that each part of the pipeline is functioning correctly before running the actual migration.



