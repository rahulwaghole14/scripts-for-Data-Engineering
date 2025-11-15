"""test creds"""

import base64
from a_common.bigquery.bigquery import create_bigquery_client


# get creds from file and base64 encode the creds
def get_creds():
    """get creds from file in this folder:"""
    creds_file_loc = "./access.json"
    with open(creds_file_loc, "r") as f:
        creds = f.read()
    return creds


def main():
    """base64 encode the creds"""
    creds = get_creds()
    creds = base64.b64encode(creds.encode()).decode()

    bigquery_client = create_bigquery_client(creds)
    print(bigquery_client)

    # query dataset: cdw_stage table: digitalsubscriptions_Commentary___Editorial
    # query = "SELECT * FROM `cdw_stage.digitalsubscriptions_Commentary___Editorial` LIMIT 100"
    # query = "select * from `hexa-data-report-etl-prod.prod_dw_intermediate.int_matrix__subscriptions` limit 10"
    query = "SELECT * FROM `hexa-data-report-etl-dev.dev_dw_intermediate.int_hexa_google_analytics__most_popular_content` LIMIT 1000"
    query_job = bigquery_client.query(query)
    results = query_job.result()
    for row in results:
        print(row)


if __name__ == "__main__":
    main()
