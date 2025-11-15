import datetime
from common.big_query.download_summarized_data import get_BQ_flow_data
from common.big_query.create_client import create_bq_client
import pandas as pd
from common.oberon_queries.queries.flow_schema import flow_processed_bq_schema
from common.big_query.upload_to_bq import biq_query_upload_drop


def process_flows():
    client = create_bq_client()
    dataset_id = "hexa_data_flow_analysis"
    output_table_id = "flow-analysis-processed"

    def download_report(replatform):
        if replatform:
            table_id = "flow-analysis-srp-new"
            output_file_name = "grouped_output.csv"
            sys_env_mapping = {
                "s200000657_65f3766facc2414a5068dcb2": "iOS",
                "s200000657_65f37661acc2414a5068dcb1": "Android",
                "s200000657_65f3764d6ef034233393eab2": "web",
            }
        else:
            table_id = "flow-analysis-old-new"
            output_file_name = "grouped_output_old.csv"
            sys_env_mapping = {
                "s200000657_65f38caaacc2414a5068dce2": "iOS",
                "s200000657_65f38c796ef034233393ead8": "Android",
                "s200000657_660361a235ef036a03d891b0": "web",
            }

        df = get_BQ_flow_data(client, dataset_id, table_id)

        df["Entry"] = df["Entry"].apply(lambda x: x.replace("'", ""))
        df["Entry"] = df["Entry"].apply(lambda x: x.replace(" ", ""))

        df["Page Type"] = df["Page Type"].apply(lambda x: x.replace(" ", ""))

        df["Entry_New"] = [
            [] if x == "Entry" else x[1:-1].split(",") for x in df["Entry"]
        ]

        def append_entry(list_, entry_):
            return list_.append(entry_)

        x = df[["Entry_New", "Page Type"]]

        df["Entry_New2"] = df.apply(
            lambda x: append_entry(x["Entry_New"], x["Page Type"]), axis=1
        )

        print(df)

        df["Date_String"] = df["Date"].apply(lambda x: x.strftime("%Y-%m-%d"))

        df_f = df.copy()

        df_f["Count"] = df_f["Entry_New"].apply(len)

        column_count = max(df_f["Count"])

        print(column_count)

        new_col_names = []
        for i in range(0, column_count):
            new_col_names.append(f"Hit_{i+1}")

        # df_f['Entry_List'] = df['Entry_New'].apply(lambda x: x[1:-1].split(','))
        df_f[new_col_names] = pd.DataFrame(df_f["Entry_New"].to_list())

        # split = pd.DataFrame(df_f['Entry_New'].to_list())

        # df_new = pd.concat([df_f,split],ignore_index=True,axis=1)

        def article_article(row):
            if len(row["Entry_New"]) > 1:
                if (
                    row["Entry_New"][-1] == "article"
                    and row["Entry_New"][-2] == "article"
                ):
                    return True
                else:
                    return False
            else:
                return False

        def article_home_article(row):
            if len(row["Entry_New"]) > 2:
                if (
                    row["Entry_New"][-1] == "article"
                    and row["Entry_New"][-2] == "home"
                    and row["Entry_New"][-3] == "article"
                ):
                    return True
                else:
                    return False
            else:
                return False

        df_f["Article_Article"] = df_f.apply(article_article, axis=1)

        df_f["Article_Home_Article"] = df_f.apply(article_home_article, axis=1)

        df_entry = df_f[df_f["Count"] == 1].copy()
        df_entry_g = (
            df_entry.groupby(by=["Date", "sys_env"])["Visits"]
            .sum()
            .reset_index()
        )
        df_entry_g.columns = ["Date", "sys_env", "Entries"]
        df_art_art = df_f[df_f["Article_Article"]].copy()
        df_art_art_g = (
            df_art_art.groupby(by=["Date", "sys_env"])["Visits"]
            .sum()
            .reset_index()
        )
        df_art_art_g.columns = ["Date", "sys_env", "Article-Article"]
        df_art_home_art = df_f[df_f["Article_Home_Article"]].copy()
        df_art_home_art_g = (
            df_art_home_art.groupby(by=["Date", "sys_env"])["Visits"]
            .sum()
            .reset_index()
        )
        df_art_home_art_g.columns = ["Date", "sys_env", "Article-Home-Article"]

        df_output = pd.merge(
            df_entry_g, df_art_art_g, on=["Date", "sys_env"], how="left"
        )
        df_output = pd.merge(
            df_output, df_art_home_art_g, on=["Date", "sys_env"], how="left"
        )

        df_output["System Environment"] = df_output["sys_env"].map(
            sys_env_mapping
        )
        return df_output

    df_replatform = download_report(True)
    df_old = download_report(False)
    df_merged = pd.concat([df_replatform, df_old], ignore_index=True)

    biq_query_upload_drop(
        client,
        df_merged,
        dataset_id,
        output_table_id,
        flow_processed_bq_schema,
    )


# print(df_output)
# df_output.to_csv(output_file_name)


# df_f.to_csv('export_srp-new.csv')
