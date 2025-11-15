import requests
import pandas as pd


def report_query(report_string):
    request_body = report_string
    report_type = "reports"
    request_url = BASE_URL + report_type + "/"
    response = requests.post(
        url=request_url, headers=REQUEST_HEADERS, data=request_body
    )
    json_response = response.json()
    if response.status_code == 200:
        df = pd.DataFrame.from_dict(json_response["rows"], orient="columns")
    else:
        df = pd.DataFrame()
    return df, json_response, response
