# Matrix Source -> BigQuery -> Braze - Data Pipeline

## Overview
This project contains a set of Python scripts that facilitate the migration of data from a Matrix SQL server to Google BigQuery. 
It includes functionalities such as creating BigQuery tables, load and update data to the bigquery tables, data validation, and more.
After the data is loaded into BigQuery, one of the specific use cases is to ingest into Braze for marketing purposes. 
A transformation DBT model is triggered after data is ingested. The DBT model is responsible for transforming the raw data into a structure to build Braze profiles and events associated to 'print' data.
The following document is divided in 2 sections: [Load Process](#load-process) and [Transform Process (DBT)](#transform-process-dbt).

## Load Process
### Prerequisites
Please install the requirements.txt before running.
`pip install -r requirements.txt`

### Key Scripts
#### `create_bigquery_tables.py`
Runs the SQL queries necessary to create the BigQuery tables with the appropriate schema. This script only needs to run at the first time of the data pipeline.

#### `load_data_to_bigquery_update.py`
Manages the transfer of data from the Matrix SQL server to BigQuery tables. Due to unreliable timestamps from the source table, data uploading and updating rely on a complete table comparison using generated hash keys for every data processing cycle.

#### `data_comparison_and_clean_up.py`
Performs data comparison between the source and the destination and executes clean-up operations to ensure data integrity. This step is crucial as it also involves deleting any source data lines from the destination that have been removed from the source.

#### `model_generator.py`
Generates Pydantic models for data validation prior to processing the data.

## Transform Process (DBT)
The Matrix DBT build process is triggered by
dbtcdwarehouse.scripts.matrixdbt Python script. 
The script is triggered from [matrix-workflow.dockerfile](../matrix-workflow.dockerfile) Docker container.

### Print account information 
The DBT model is responsible for transforming the raw data into a structure to build Braze profiles and events associated to 'print' data.
The following query provides the print account information data used to build the print_account_information custom attribute in Braze.

```select * from `hexa-data-report-etl-dev.rbhaskhar_dw_intermediate.int_matrix__to_braze_print_account_information`;```

### Print communication subscriptions
```select * from `hexa-data-report-etl-dev.rbhaskhar_dw_intermediate.int_matrix__to_braze__matrix_user_profiles_print_communication_subscriptions`;```

### Print related events
```select * from `hexa-data-report-etl-dev.rbhaskhar_dw_intermediate.int_matrix__to_braze__matrix_events_combined`;```

## General design - Braze profiles use case
For a general design diagram related to the generation of print profile for Braze see [Digital and Print Braze profiles](https://hexanz.atlassian.net/wiki/spaces/MAT/whiteboard/3521216520)