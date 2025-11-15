def get_BQ_table_dates(client, bq_dataset, bq_table):
    output_list = []
    query = (
        "SELECT date FROM `hexa-data-report-etl-prod."
        + bq_dataset
        + "."
        + bq_table
        + "` GROUP BY date"
    )
    job = client.query(query)
    for row in job.result():
        # print(row[0])
        # output_list.append(row[0])
        output_list.append(row[0].strftime("%Y-%m-%d"))
    return output_list
