#!/usr/bin/env python
# coding: utf-8
"""This script provides methods to convert websocket JSON data into dataframe."""

import pandas as pd

# Variables from websocket JSON files
RRI_MEASUREMENT_NAME = "RrInterval"
TIMESTAMP_DATAFRAME_COL_NAME = "timestamp"
X_ACM_DATAFRAME_COL_NAME = "x_acm"
Y_ACM_DATAFRAME_COL_NAME = "y_acm"
Z_ACM_DATAFRAME_COL_NAME = "z_acm"
X_GYRO_DATAFRAME_COL_NAME = "x_gyro"
Y_GYRO_DATAFRAME_COL_NAME = "y_gyro"
Z_GYRO_DATAFRAME_COL_NAME = "z_gyro"
SENSIBILITY_DATAFRAME_COL_NAME = "sensibility"
JSON_DATA_FIELD_NAME = "data"

def convert_acm_json_to_df(acm_json: dict) -> pd.DataFrame:
    """
    Function converting accelerometer JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - Accelerometer JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), acm_json[JSON_DATA_FIELD_NAME]))

    columns = [TIMESTAMP_DATAFRAME_COL_NAME, X_ACM_DATAFRAME_COL_NAME, Y_ACM_DATAFRAME_COL_NAME,
               Z_ACM_DATAFRAME_COL_NAME, SENSIBILITY_DATAFRAME_COL_NAME]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index(TIMESTAMP_DATAFRAME_COL_NAME)

    # Convert string to numeric values
    df_to_write[[X_ACM_DATAFRAME_COL_NAME,
                 Y_ACM_DATAFRAME_COL_NAME,
                 Z_ACM_DATAFRAME_COL_NAME]] = df_to_write[[X_ACM_DATAFRAME_COL_NAME,
                                                           Y_ACM_DATAFRAME_COL_NAME,
                                                           Z_ACM_DATAFRAME_COL_NAME]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_rri_json_to_df(rri_json) -> pd.DataFrame:
    """
    Function converting RrInterval JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - RrInterval JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), rri_json[JSON_DATA_FIELD_NAME]))

    columns = [TIMESTAMP_DATAFRAME_COL_NAME, RRI_MEASUREMENT_NAME]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index(TIMESTAMP_DATAFRAME_COL_NAME)

    # Convert string to numeric values
    df_to_write[RRI_MEASUREMENT_NAME] = df_to_write[RRI_MEASUREMENT_NAME].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write


def convert_gyro_json_to_df(gyro_json) -> pd.DataFrame:
    """
    Function converting gyroscope JSON data to pandas Dataframe.

    Arguments
    ---------
    acm_json - gyroscope JSON file sent from Web-socket

    Returns
    ---------
    df_to_write - Dataframe to write in influxDB
    """
    # Extract values to write to InfluxDB
    extracted_data_from_json_file = list(map(lambda x: x.split(" "), gyro_json[JSON_DATA_FIELD_NAME]))

    columns = [TIMESTAMP_DATAFRAME_COL_NAME, X_GYRO_DATAFRAME_COL_NAME, Y_GYRO_DATAFRAME_COL_NAME, Y_GYRO_DATAFRAME_COL_NAME]
    df_to_write = pd.DataFrame(extracted_data_from_json_file, columns=columns).set_index(TIMESTAMP_DATAFRAME_COL_NAME)

    # Convert string to numeric values
    df_to_write[[X_GYRO_DATAFRAME_COL_NAME,
                 Y_GYRO_DATAFRAME_COL_NAME,
                 Z_GYRO_DATAFRAME_COL_NAME]] = df_to_write[[X_GYRO_DATAFRAME_COL_NAME,
                                                            Y_GYRO_DATAFRAME_COL_NAME,
                                                            Z_GYRO_DATAFRAME_COL_NAME]].apply(pd.to_numeric)
    # Convert index to datetime index
    df_to_write.index = pd.to_datetime(df_to_write.index)
    return df_to_write
