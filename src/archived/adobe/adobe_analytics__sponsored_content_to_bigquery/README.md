# Adobe Analytics Data Extraction for Sponsored Contents

## Installation
*Python 3 is required.*

    python -m venv venv
    Windows: source venv/Scripts/activate
    Linux: venv/Scripts/activate.bat
    pip install --upgrade pip setuptools wheel
    pip install -U -r requirements.txt

## Authentication
To authenticate in Adobe Analytics, you will need to get these files from your administrator.

* secrets/private.key

    `--RSA PRIVATE KEY--`

* secrets/secret_values.json

    `{"X_API_KEY": "","CLIENT_SECRET": "","ISS": "", "SUB": "","AUD": ""}`

* secrets/access.json

    `{"token_type": "bearer", "access_token": "this_is_a_sample_token", "expires_in": 123}`

`python create_credentials.py`

## Usage
    git clone https://github.com/hexaNZ/hexa-data-sponsored-content.git

### Summary
* `main.py` runs the adobe analytics data extraction for the Sponsored contents listed in the <a href="https://docs.google.com/spreadsheets/d/1Ou0GTsQa8Xg01YSUh9NLTRCzN2Xj33FvH1ew50Yqywc/edit?usp=sharing">
Sponsored content campaign</a>. The extracted data is stored in the following two BigQuery tables:

  + `hexa-data-report-etl-prod.sponsored_content.mobile_device`: Stores daily data breakdown by
  Mobile device for each sponsored content.
  + `hexa-data-report-etl-prod.sponsored_content.regional`: Stores daily data breakdown by domestic region for each
  sponsored content.
  + The metrics used are:
    + Page Views
    + Unique Visitors
    + Average time spent (sec)
    + Time Spent per Visitor (sec)
    + Time Spent per Visit (sec)
* `extract_dates.py` extracts missing dates from log files or BigQuery tables that will be used as parameters for data
  extraction in Adobe Analytics API.
* `extract_data.py` extracts data for each BigQuery table using Adobe Analytics API.
* `bigquery_module.py` has modules required to handle BigQuery operations.
* `logging_module.py` has modules required to handle logging operations.
* `create_credential.py` generates access files for API operations using the Adobe Analytics project's private key and
 secret_values.
* `missing_data.py` & `extract_data_fix.py` extract missing data within the specified date range and add it to the BigQuery tables.

### TODO:
* Breakdown of sponsored content by BT (AAM Behavioral targeting) segments.
This will be done at the request of stakeholders. Allie Sinclair & Amanda Montgomerie are the main stakeholder in this
request.

### Related links
* <a href="https://hexanz.atlassian.net/browse/DATA-1461">DATA-1461: Standardization of Sponsored Content ETL</a>
* <a href="https://hexanz.atlassian.net/browse/DATA-1447">DATA-1447: Sponsored content audience analysis</a>
* <a href="https://hexanz.atlassian.net/browse/DATA-1449">DATA-1449: Sponsored Content Dashboard with Audience data</a>
* <a href="https://docs.google.com/spreadsheets/d/1Ou0GTsQa8Xg01YSUh9NLTRCzN2Xj33FvH1ew50Yqywc/edit?usp=sharing">Google sheet: Sponsored content campaign</a>
* <a href="https://www4.an.adobe.com/x/4_5qf0v">Adobe Analytics workspace: SC audience analysis</a>
* <a href="https://datastudio.google.com/s/huOO9bRC17g">Dashboard: hexa Sponsored Content</a>
