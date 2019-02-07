#!/usr/bin/env python
# coding: utf-8
"""This script provides methods to process RR-interval JSON files."""

import json
import datetime
import numpy as np
import pandas as pd
from dataframe_converter import convert_rri_json_to_df

TYPE_PARAM_NAME = "type"

def concat_rrinterval_files_into_single_dataframe(files_list: list) -> pd.DataFrame:
    """
    Concatenate JSON files content into a single pandas DataFrame.

    Arguments
    ---------
    files_list - list of files to sort

    Returns
    ---------
    concatenated_rr_interval_dataframe - resulting pandas DataFrame
    """
    dataframe_list = []
    for file in files_list:
        # Open Json file
        with open(file) as json_file:
            json_data = json.load(json_file)

        # Get tags from file
        measurement = json_data[TYPE_PARAM_NAME]

        # Extract data and create dataframe from JSON file
        if measurement == "RrInterval":
            rr_interval_dataframe = convert_rri_json_to_df(json_data)
            dataframe_list.append(rr_interval_dataframe)

    # Concat list of dataframe
    concatenated_rr_interval_dataframe = pd.concat(dataframe_list)
    return concatenated_rr_interval_dataframe


def create_corrected_timestamp_list(concatenated_rr_interval_dataframe: pd.DataFrame) -> list:
    """
    Create a corrected timestamp based on cumulative sum of RR-intervals values.

    Arguments
    ---------
    concatenated_df - pandas DataFrame containing all data of a specific user

    Returns
    ---------
    corrected_timestamp_list - Corrected timestamp generated
    """
    rri_list = concatenated_rr_interval_dataframe["RrInterval"].values
    polar_index = concatenated_rr_interval_dataframe.index

    current_timestamp = polar_index[0]
    next_timestamp = polar_index[1]

    # Set the first timestamp to be the first timestamp of the polar
    corrected_timestamp_list = [current_timestamp]

    for i in range(1, len(polar_index) - 1):
        next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                      corrected_timestamp_list[-1], rri_list[i])
        corrected_timestamp_list.append(next_corrected_timestamp)

        # Update next timestamps to compute time difference
        current_timestamp = polar_index[i]
        next_timestamp = polar_index[i+1]

    # Deal with last timestamp value
    next_corrected_timestamp = get_next_timestamp(next_timestamp, current_timestamp,
                                                  corrected_timestamp_list[-1], rri_list[-1])
    corrected_timestamp_list.append(next_corrected_timestamp)

    return corrected_timestamp_list


def get_next_timestamp(next_timestamp, current_timestamp, last_corrected_timestamp,
                       next_rr_interval: float):
    """

    :param next_timestamp:
    :param current_timestamp:
    :param last_corrected_timestamp:
    :param next_rr_interval:
    :return:
    """
    # Deal with last timestamp value
    time_difference = next_timestamp - current_timestamp
    if abs(time_difference.seconds) < 3:
        next_corrected_timestamp = last_corrected_timestamp + \
                                   datetime.timedelta(milliseconds=np.float64(next_rr_interval))
        return next_corrected_timestamp
    else:
        return next_timestamp
