import pandas as pd


def get_BQ_summarized_data(client, bq_dataset, bq_table):
    #output_list = []
    query = "SELECT \
        `Page Id` AS `Content ID`, \
        SUM(`Unique Visitors`) AS `Unique Visitors`, \
        SUM(`Page Views`) AS `PVs`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Business`) AS `UVs Business`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Informed`) AS `UVs Informed`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Lifestyle`) AS `UVs Lifestyle`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - National`) AS `UVs National`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Power Users`) AS `UVs Power Users`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Regional Browsers`) AS `UVs Regional Browsers`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Regional Indepth`) AS `UVs Regional Indepth`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Skimmers`) AS `UVs Skimmers`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - Sports`) as `UVs Sports`, \
        SUM(`Unique Visitors - Segment: Test Segmentation - World`) AS `UVs World`, \
        SUM(`Unique Visitors - Segment: Visitor - Single Visit SRP`) AS `UVs Single Visit`, \
        SUM(`Page Views - Breakdown: Referrer Type - 6`) AS `PVs Type - 6`, \
        SUM(`Page Views - Breakdown: Referrer Type - 3`) AS `PVs Type - 3`, \
        SUM(`Page Views - Breakdown: Referrer Type - 2`) AS `PVs Type - 2`, \
        SUM(`Page Views - Breakdown: Referrer Type - 9`) AS `PVs Type - 9`, \
        SUM(`Video Views`) AS `Video Views`, \
        SUM(`Entries`) AS `Entries`, \
        SUM(`Bounces`) AS `Bounces`, \
        MIN(`date`) AS `min_date` \
    FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "` GROUP BY `Page Id`"
    
    df = client.query(query).to_dataframe()

    return df



def get_BQ_flow_data(client, bq_dataset, bq_table):
    #output_list = []
    query = "SELECT * \
    FROM `hexa-data-report-etl-prod." + bq_dataset + "." + bq_table + "`"
    
    df = client.query(query).to_dataframe()

    return df