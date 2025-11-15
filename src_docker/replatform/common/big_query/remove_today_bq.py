import pandas as pd


def delete_day_from_table(client, bq_dataset, bq_table, date_):
    #output_list = []
    query = "DELETE \
    FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "` WHERE date = " + \
    date_
    
    a = client.query(query)
    print(a)
    return a