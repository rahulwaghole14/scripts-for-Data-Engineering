# Showcase Plus Data Updater

This repository contains two Python scripts for updating the Showcase Plus data from Google Sheets. These scripts automate the data handling process, ensuring data from Google Sheets is efficiently validated and uploaded to Google BigQuery.

## Scripts Description

- **`showcase_plus_g_sheet_backfill.py`**: Used for backfilling Showcase Plus data. This is necessary since the data has not been updated since July 2023.
- **`showcase_plus_g_sheet_monthly_import.py`**: Automates the process of fetching, validating, and uploading data from Google Sheets to Google BigQuery on a monthly basis.

## Features

- Retrieves data from specified Google Sheets.
- Validates data using predefined schemas.
- Uploads validated data to Google BigQuery.
- Handles errors gracefully and logs them for troubleshooting.
- Utilizes environmental variables for secure access to APIs.
