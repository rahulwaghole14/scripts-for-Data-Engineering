# adobe-2.0

Packages Needed:
* cryptography
* pandas
* pyjwt

Files needed:
* private.key in /secrets/
* secret_values.json in /secrets/ containing "X_API_KEY", "CLIENT_SECRET", "ISS", "SUB" & "AUD" values
* create secrets/access.json with {"token_type": "bearer", "access_token": "", "expires_in": 1}

Authentication:
* Run create_credentials.py

Endpoints:
1. Reports

You can call the report endpoint with report_query using either a json string or using report_string function and start_date,end_date, metricFilterSegments as variables.

Json string queries can be taken from Abode Debug tool encased in triple quotes.

Report_query returns 3 values:
    [dataframe based on 'rows', the json reponse, the reponse code]

The dataframe output may need some wrangling to get into a usable format.
