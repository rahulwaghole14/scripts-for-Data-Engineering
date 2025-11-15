# Overview - hexa-data-revenue-share-reports

    This script creates daily impressions files by Key values using the GAM Report Manager API for the Revenue Share Reports. You can find the credentials in the .env file.

## Installation and Setup
    Please install the requirements.txt before running.

##  Scripts
    There are two types of scripts:
### Type 1:
    Designed to automate the process of fetching data from Google Ad Manager, processing it, and loading it into Google BigQuery. It handles tasks like checking if data is already loaded for specific dates, deleting data for given date ranges, and loading new data. The script uses the googleads and google-cloud-bigquery libraries for API interactions. The main scripts related to this part are:
    1, 'run_saved_report.py', 'Load_data_to_bq.py': Utility scripts for loading data into BigQuery.Includes functions for dataset and table creation, data loading, and error handling.
    2, 'run_pca_gen.py', 'run_pca_hourly.py', 'run_pca_view.py':These scripts are part of a series handling different aspects of data processing and loading. Each script is tailored for specific data types and intervals.
    3, 'run_vv_gen.py', 'run_vv_sellthrough.py': Similar to the PCA scripts, these handle varied data processes. They focus on different metrics and data points relevant to the project.

### Type 2:
    'PCAadwallet.py': This script processes new line item IDs from Google BigQuery, fetches associated data from an external API, and then loads this data back into BigQuery. It handles the deduplication of IDs and ensures that only new or unprocessed IDs are fetched and processed. The script is configured to log its activities to both the console and a file.

## Process
    The alteryx scheduler is set up for running every morning at 6 AM. The estimated run time is around 1 to 2 hours. The related files are:
    1, 'main.py':
    Main script that orchestrates the workflow, calling other scripts as needed.
    Includes error handling and logging mechanisms.
    2, 'main.bat':
    A batch file designed to execute the main.py script.
    Useful for scheduling and automating the script's execution.


## old bat file
```
@echo off

REM Change to your project directory
cd "C:\projects\hexa-data-alteryx-workflows" || exit /b

REM Activate the Conda environment
call C:\ProgramData\miniconda3\Scripts\activate.bat python312

REM Run python file
echo Running python file
python src\google_ad_manager__to_bigquery\main.py

REM Check error level returned by Python script and raise to alteryx
if %ERRORLEVEL% neq 0 (
    echo Python script failed.
    exit /b 1
)

echo Done.

```
