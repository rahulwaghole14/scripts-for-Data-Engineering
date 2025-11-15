# Press Patron Data Processing Workflow

This repository contains scripts for extracting Press Patron data (subscriptions, users, transactions) via their API and processing it for use in marketing and analytics workflows.

## Repository Contents

- `pressPatronAPI_new.py`: Extracts subscriptions, users, and transaction data from the Press Patron API and uploads it to a BigQuery database located in Australia. This script handles the initial data ingestion.

- `generate_uuid.py`: Generates marketing IDs, upserts them into the `prod_dw_intermediate.int_presspatron__bq_to_braze_user_profile` table in BigQuery, ensuring each user profile has a unique identifier.

- `pressPatronAPI_au.yxmd`: An Alteryx workflow designed for the automated updating of the Press Patron data and downstream tables, set to execute hourly.

## Data Transformation

Data transformation is performed using dbt. The dbt scripts are located in a separate repository: [hexa-data-cdwarehouse](https://github.com/hexaNZ/hexa-data-cdwarehouse). The following intermediate data models have been created:

- `prod_dw_intermediate.int_presspatron__bq_to_braze_user_profile`
- `prod_dw_intermediate.int_presspatron__cancellation`
- `prod_dw_intermediate.int_presspatron__donations`
- `prod_dw_intermediate.int_presspatron__newsletter`

## High Touch Data Models

The `prod_dw_intermediate.int_presspatron__bq_to_braze_user_profile` table is the source for two data models in High Touch:

- [High Touch Model bq_to_braze__presspatron_sub_status](https://app.hightouch.com/hexa-prod-51lz3/models/1658685)
- [High Touch Model bq_to_braze_presspatron_user_profile](https://app.hightouch.com/hexa-prod-51lz3/models/1658683)

These models facilitate the processing of data to Braze for marketing purposes.

