# adobe_analytics

The dbt model is designed perform ELT process on adobe analytics hit data. It is done in the following stages:


# source of data

There is a process which moves data from a gcs bucket into BigQuery tables. There tables are in the Dataset ID: hexa-data-report-etl-prod.cdw_raw_adobe_analytics_masthead. These conatin the hit data as well as all lookup tables. The hit data has the table format 'data_yyyymmddhh' where yyyymmddhh is the timestamp associated with the datafeed file. Details regarding the data lineage are aviable here under 'Adobe Analytics Datafeed' https://hexanz.atlassian.net/wiki/spaces/CD/pages/2810183711/Data+Lineage


# schema of source data
[refernce docs](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-reference.html?lang=en)

[data feed best practises](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feeds-best-practices.html?lang=en)

[data feed overview](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-overview.html?lang=en)

[Use data feeds to calculate common metrics
](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-calculate.html?lang=en)

[Data feed contents - overview
](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-contents.html?lang=en)

# Stage

## stg_adobe_analytics__hits.sql

1. Get Latest Table Suffix from the Sat Table.
2. Based on the value , ingest  all raw tables into stage which have a suffix greater than that value.
3. Left Join necessary lookup tables .
4. Cast/Convert 'date_time' and 'TABLE_SUFFIX" from String to Datetime
5. Concatinate values to create an Adobe_key which will serve as a unique identifier for each row

## vlt_stg_adobe_analytics__hits

 Prepare Data to be staged into the Data Vault

# Intermediate / VAULT

# DATA MART
