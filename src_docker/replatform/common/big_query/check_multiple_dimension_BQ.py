import pandas as pd



def get_BQ_table_multiple_dims(client, bq_dataset, bq_table):
    output_list = []
    query = "SELECT Vertical, `System Environment`, Resolution, Region, `Page Type` FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "` GROUP BY Vertical, `System Environment`, Resolution, Region, `Page Type`"
    job = client.query(query)
    for row in job.result():
        #print(row[0])
        #output_list.append(row[0])
        #print(row)
        #print(row[0])
        output_list.append(str(row[0]) + '-' + str(row[1]) + '-' + str(row[2]) + '-' + str(row[3]) + '-' + str(row[4]))
    return output_list

