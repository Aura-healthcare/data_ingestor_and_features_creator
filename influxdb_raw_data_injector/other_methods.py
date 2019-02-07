#!/usr/bin/env python
# coding: utf-8
"""This script provides some other methods useful for the ingestor."""

import datetime
import pandas as pd

# ---------------- OTHER METHODS ---------------- #

def create_df_with_unique_index(data_to_write: pd.DataFrame,
                                time_delta_to_add: int = 123456) -> pd.DataFrame:
    """
    Function creating a new Dataframe with a unique index to avoid points overwrite in time series DB.

    Arguments
    ---------
    data_to_write - data to inject in influxDB
    time_delta_to_add - timedelta to add, in ns, to avoid having 2 points with same timestamp index

    Returns
    ---------
    data_with_unique_index - pandas Dataframe with unique index to avoid overwritten points
    in influxDB
    """
    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    while not is_index_unique:
        data_to_write.index = data_to_write.index.where(~data_to_write.index.duplicated(),
                                                        data_to_write.index + pd.to_timedelta(time_delta_to_add,
                                                                                              unit='ns'))
        data_to_write = data_to_write.sort_index()
        is_index_unique = data_to_write.index.is_unique
    return data_to_write


def create_files_by_user_dict(files_list: list) -> dict:
    """
    Create a dictionary containing the corresponding list of RR-inteval files for each user.

    Arguments
    ---------
    files_list - list of files, must be sorted for function to operate correctly !

    Returns
    ---------
    files_by_user_dict - sorted dictionary

    ex :
    files_by_user_dict = {
            'user_1': ["file_1", "file_2", "file_3"],
            'user_2': ["file_4", "file_5"],
            'user_3': ["file_6", "file_7", "file_8"]
    }
    """
    # Create sorted user list
    user_list = list(set(map(lambda x: x.split("/")[-1].split("_")[0], files_list)))
    user_list.sort()

    files_by_user_dict = dict()
    file_list_for_a_user = []
    try:
        current_user = user_list[0]
    except IndexError:
        return dict()

    for filename in files_list:
        if current_user in filename:
            file_list_for_a_user.append(filename)
        else:
            files_by_user_dict[current_user] = file_list_for_a_user
            current_user = filename.split("/")[-1].split("_")[0]
            file_list_for_a_user = [filename]

    # Add list of files for last user in dictionary
    files_by_user_dict[current_user] = file_list_for_a_user
    return files_by_user_dict


def launch_retroactive_influxdb_cq(client, user, measurement, first_timestamp):
    """

    :param client:
    :param user:
    :param measurement:
    :param first_timestamp:
    :return:
    """
    # Create Unix timestamp for retroactive query
    unix_timestamp_for_query = datetime.datetime(*first_timestamp.timetuple()[:3]).strftime("%s") + "000ms"

    # Create retroactive continuous queries if data has been ingested for more than 1d
    if measurement == "MotionAccelerometer":
        retroactive_continuous_query = "SELECT count(\"x_acm\") INTO \"x_acm_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
        retroactive_continuous_query = "SELECT count(\"y_acm\") INTO \"y_acm_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
        retroactive_continuous_query = "SELECT count(\"z_acm\") INTO \"z_acm_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
    elif measurement == "MotionGyroscope":
        retroactive_continuous_query = "SELECT count(\"x_gyro\") INTO \"x_gyro_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
        retroactive_continuous_query = "SELECT count(\"y_gyro\") INTO \"y_gyro_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
        retroactive_continuous_query = "SELECT count(\"z_gyro\") INTO \"z_gyro_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)
    else:
        retroactive_continuous_query = "SELECT count(\"RrInterval\") INTO \"rri_count_by_day\" FROM {} WHERE \"user\" = '{}' and time >= {} GROUP BY time(1d), \"user\"".format(measurement, user, unix_timestamp_for_query)
        client.query(retroactive_continuous_query)

