import numpy as np
import pandas as pd


def map_dataframe(data_frame, schema_map_dict):
    """
    Map the dataframe to the bigquery schema
    """
    for key, value in schema_map_dict.items():
        # iterate through the schema map dict and rename the columns from source to target
        data_frame.rename(
            columns={value["source"]: value["target"]}, inplace=True
        )

        # set data types from schema_map_dict
        if value["type"] == "integer":
            data_frame[value["target"]] = (
                pd.to_numeric(data_frame[value["target"]], errors="coerce")
                .fillna(0)
                .astype(int)
            )
        elif value["type"] == "float":
            data_frame[value["target"]] = (
                data_frame[value["target"]].fillna(0.0).astype(float)
            )
        elif value["type"] == "string":
            data_frame[value["target"]] = (
                data_frame[value["target"]].fillna("(not set)").astype(str)
            )
        # use dataframe_type to set the data type if present in the schema_map_dict
        # else use type
        else:
            if "dataframe_type" in value:
                data_frame[value["target"]] = data_frame[
                    value["target"]
                ].astype(value["dataframe_type"])
            else:
                data_frame[value["target"]] = data_frame[
                    value["target"]
                ].astype(value["type"])

    return data_frame
