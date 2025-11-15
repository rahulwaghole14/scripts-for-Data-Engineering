"""blah"""
# pylint: disable=all

from datetime import timedelta, datetime
import pandas as pd
import logging

# big query
from google.cloud import bigquery

# adobe analytics
# from common.reports.report_query import initialise_query
# from common.reports.report_query import get_report_query
from .common.reports.report_query import get_report_query_limit
from .common.reports.report_query import initialise_query_date

# from common.reports.report_query import initialise_query_date, initialise_query_between_dates
from .common.reports.query import Metric
from .logging_module import logger
from .bigquery_module import *

logger()


def process_request(df, metric_col_names, value_col_name, drop_itemid):
    """
    Process dataframe response by Adobe API 2.0.

    :param df: The original dataframe
    :type df: Pandas dataframe

    :param metric_col_names: List of the metric column names
    :type metric_col_names: List of strings

    :param value_col_name: The name assigned to the column 'value'
    :type value_col_name: string

    :param drop_itemid: 1 (drop itemID) or 0 (keep itemID for further processing). The 'itemID' is the unique identifier
    of items in Adobe Analytics.
    :type drop_itemid: bool

    :returns: The processed dataframe
    :rtype: Pandas dataframe
    """
    try:
        df[metric_col_names] = pd.DataFrame(df.data.values.tolist())
        if drop_itemid == 1:
            df = df.drop(["data", "itemId"], axis=1)
        df.rename(columns={"value": value_col_name}, inplace=True)

        return df
    except:
        logging.info("Processing request failed")
        return None


def extract_data_for_mobile_device(table_id, client, start_date, end_date):

    """
    Extract data for the BigQuery table: mobile_device.

    :param table_id: The ID of the BigQuery table
    :type table_id: string

    :param client: BigQuery client object
    :type client: object

    :param start_date: The start date to extract data for the mobile_device table
    :type start_date: datetime64

    :param end_date: The end date to extract data for the mobile_device table
    :type end_date: datetime64

    :returns: None
    """
    try:
        bq_schema = [
            bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField(
                "ContentID", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Mobile_device", bigquery.enums.SqlTypeNames.STRING
            ),
            bigquery.SchemaField(
                "Page_Views", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Unique_Visitors", bigquery.enums.SqlTypeNames.INT64
            ),
            bigquery.SchemaField(
                "Average_time_spent", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Time_spent_per_visitor", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Time_spent_per_visit", bigquery.enums.SqlTypeNames.FLOAT64
            ),
            bigquery.SchemaField(
                "Load_datetime", bigquery.enums.SqlTypeNames.DATETIME
            ),
        ]

        logging.info("Creating Query for Mobile Device Data")
        # s200000657_55d27bbae4b04cb8a91e784f -> Countries = NZ
        segment_id = ["s200000657_5db8a82471ac0108a6900555"]

        metric1 = Metric("metrics/pageviews", 0)
        metric2 = Metric("metrics/visitors", 1)
        metric3 = Metric("cm_average_time_on_site_defaultmetric", 2)
        metric4 = Metric("metrics/timespentvisitor", 3)
        metric5 = Metric("metrics/timespentvisit", 4)
        # metric6 = Metric('metrics/bouncerate', 5)

        metrics = [metric1, metric2, metric3, metric4, metric5]
        metric_col_names = [
            "Page_Views",
            "Unique_Visitors",
            "Average_time_spent",
            "Time_spent_per_visitor",
            "Time_spent_per_visit",
        ]

        dimension = "variables/mobiledevicetype"
        breakdown_dimension = {}

        logging.info("Initializing queries for main dimension")
        # start_date = date(2022,1,1)
        # end_date = date(2022,4,30)
        # get the list of missing dates or number of days
        nod = (end_date - start_date).days
        logging.info(nod)

        for n in range(nod):
            end_date = start_date + timedelta(days=1)
            merge = pd.DataFrame()
            logging.info(
                "{}, start date = {}, end date = {}".format(
                    n + 1, start_date, end_date
                )
            )

            dimension = "variables/mobiledevicetype"
            breakdown_dimension = {}

            initialise_query_date(
                dimension=dimension,
                breakdown_dimension=breakdown_dimension,
                segment_id=segment_id,
                dates=start_date,
                metrics=metrics,
            )

            df_itemid = get_report_query_limit()

            if not df_itemid.empty:
                df_itemid = process_request(
                    df_itemid, metric_col_names, "Mobile_device", 0
                )
                logging.info(
                    "{0} Content IDs found for the segment_id {1}".format(
                        df_itemid.shape[0], segment_id
                    )
                )

                for index, row in df_itemid.iterrows():
                    dimension = "variables/prop11"
                    breakdown_dimension = {
                        "type": "breakdown",
                        "dimension": "variables/mobiledevicetype",
                        "itemId": str(row["itemId"]),
                    }

                    initialise_query_date(
                        dimension=dimension,
                        breakdown_dimension=breakdown_dimension,
                        segment_id=segment_id,
                        dates=start_date,
                        metrics=metrics,
                    )

                    df = get_report_query_limit()

                    if not df.empty:
                        #  df = process_request(df)
                        df = process_request(
                            df, metric_col_names, "ContentID", 1
                        )
                        logging.info(df.head())
                        logging.info(row)
                        df["Mobile_device"] = row["Mobile_device"]
                        df["Date"] = start_date
                        merge = merge.append(df)
                        logging.info(
                            "index = {}; dataframe size = {}, merge size = {}".format(
                                index, df.shape, merge.shape
                            )
                        )

                        logging.info(df.columns)
                        logging.info(df.shape)
                        logging.info(df.head())

            if not merge.empty:
                merge.dropna(subset=["ContentID"], inplace=True)
                merge["Load_datetime"] = datetime.now()
                columns_to_int = ["ContentID", "Page_Views", "Unique_Visitors"]
                merge[columns_to_int] = merge[columns_to_int].astype("int64")
                # merge[columns_to_float] = merge[columns_to_float].astype('float64')
                merge["Date"] = pd.to_datetime(merge["Date"]).dt.date

                logging.info(merge.info())

                columns_order = [
                    "Date",
                    "ContentID",
                    "Mobile_device",
                    "Page_Views",
                    "Unique_Visitors",
                    "Average_time_spent",
                    "Time_spent_per_visitor",
                    "Time_spent_per_visit",
                    "Load_datetime",
                ]
                merge = merge[columns_order]

                logging.info(merge.head())
                logging.info(merge.info())

                # merge.to_csv(os.getcwd()+"\\nbly_url_level_data_"+str(start_date)+".csv")
                append_dataframe_to_bigquery(
                    merge, client, table_id, bq_schema, start_date
                )
                # overwrite_dataframe_to_bigquery(merge, client, table_id, bq_schema)

            start_date = end_date
    except Exception as e:
        logging.error(
            f"An error occurred in extract_data_for_mobile_device: {e}"
        )


def extract_data_for_regional(table_id, client, start_date, end_date):
    """
    Extract data for the BigQuery table: regional.

    :param table_id: The ID of the BigQuery table
    :type table_id: string

    :param client: BigQuery client object
    :type client: object

    :param start_date: The start date to extract data for the regional table
    :type start_date: datetime64

    :param end_date: The end date to extract data for the regional table
    :type end_date: datetime64

    :returns: None
    """
    bq_schema = [
        bigquery.SchemaField("Date", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("ContentID", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("Regions", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Page_Views", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField(
            "Unique_Visitors", bigquery.enums.SqlTypeNames.INT64
        ),
        bigquery.SchemaField(
            "Average_time_spent", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "Time_spent_per_visitor", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "Time_spent_per_visit", bigquery.enums.SqlTypeNames.FLOAT64
        ),
        bigquery.SchemaField(
            "Load_datetime", bigquery.enums.SqlTypeNames.DATETIME
        ),
    ]

    logging.info("Creating Query for Regional Data")
    # s200000657_55d27bbae4b04cb8a91e784f -> Countries = NZ
    segment_id = [
        "s200000657_5db8a82471ac0108a6900555",
        "s200000657_566f38e9e4b080432935e591",
    ]

    metric1 = Metric("metrics/pageviews", 0)
    metric2 = Metric("metrics/visitors", 1)
    metric3 = Metric("cm_average_time_on_site_defaultmetric", 2)
    metric4 = Metric("metrics/timespentvisitor", 3)
    metric5 = Metric("metrics/timespentvisit", 4)
    # metric6 = Metric('metrics/bouncerate', 5)

    metrics = [metric1, metric2, metric3, metric4, metric5]
    metric_col_names = [
        "Page_Views",
        "Unique_Visitors",
        "Average_time_spent",
        "Time_spent_per_visitor",
        "Time_spent_per_visit",
    ]

    dimension = "variables/georegion"
    breakdown_dimension = {}

    logging.info("Initializing queries for main dimension")
    # start_date = date(2022,1,1)
    # end_date = date(2022,4,30)
    # get the list of missing dates or number of days
    nod = (end_date - start_date).days
    logging.info(nod)

    for n in range(nod):
        end_date = start_date + timedelta(days=2)
        merge = pd.DataFrame()
        logging.info(
            "{}, start date = {}, end date = {}".format(
                n + 1, start_date, end_date
            )
        )

        dimension = "variables/georegion"
        breakdown_dimension = {}

        initialise_query_date(
            dimension=dimension,
            breakdown_dimension=breakdown_dimension,
            segment_id=segment_id,
            dates=start_date,
            metrics=metrics,
        )

        df_itemid = get_report_query_limit()

        if not df_itemid.empty:
            df_itemid = process_request(
                df_itemid, metric_col_names, "Regions", 0
            )
            logging.info(
                "{0} Content IDs found for the segment_id {1}".format(
                    df_itemid.shape[0], segment_id
                )
            )

            for index, row in df_itemid.iterrows():
                dimension = "variables/prop11"
                breakdown_dimension = {
                    "type": "breakdown",
                    "dimension": "variables/georegion",
                    "itemId": str(row["itemId"]),
                }

                initialise_query_date(
                    dimension=dimension,
                    breakdown_dimension=breakdown_dimension,
                    segment_id=segment_id,
                    dates=start_date,
                    metrics=metrics,
                )

                df = get_report_query_limit()
                logging.info(df.head())

                if not df.empty:
                    # df = process_request(df)
                    df = process_request(df, metric_col_names, "ContentID", 1)
                    df["Regions"] = row["Regions"]
                    df["Date"] = start_date
                    merge = merge.append(df)
                    logging.info(
                        "index = {}; dataframe size = {}, merge size = {}".format(
                            index, df.shape, merge.shape
                        )
                    )

                    logging.info(df.columns)
                    logging.info(df.shape)
                    logging.info(df.head())

        if not merge.empty:
            merge.dropna(subset=["ContentID"], inplace=True)
            merge["Load_datetime"] = datetime.now()
            columns_to_int = ["ContentID", "Page_Views", "Unique_Visitors"]
            merge[columns_to_int] = merge[columns_to_int].astype("int64")
            # merge[columns_to_float] = merge[columns_to_float].astype('float64')
            merge["Date"] = pd.to_datetime(merge["Date"]).dt.date

            columns_order = [
                "Date",
                "ContentID",
                "Regions",
                "Page_Views",
                "Unique_Visitors",
                "Average_time_spent",
                "Time_spent_per_visitor",
                "Time_spent_per_visit",
                "Load_datetime",
            ]
            merge = merge[columns_order]

            logging.info(merge.head())
            logging.info(merge.info())

            # merge.to_csv(os.getcwd()+"\\nbly_url_level_data_"+str(start_date)+".csv")
            append_dataframe_to_bigquery(
                merge, client, table_id, bq_schema, start_date
            )
            # overwrite_dataframe_to_bigquery(merge, client, table_id, bq_schema)

        start_date = end_date
