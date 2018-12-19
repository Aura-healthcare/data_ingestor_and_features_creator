#!/usr/bin/env python
# coding: utf-8
"""This script defines methods to compute features from InfluxDB Data"""

import json
from datetime import timezone
import math
from typing import List
import datetime
import configparser
import numpy as np
import pandas as pd
from influxdb import InfluxDBClient
from influxdb import DataFrameClient

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

ACCELEROMETER_MEASUREMENT_NAME = "MotionAccelerometer"


# --------------------- FUNCTIONS TO QUERY INFLUXDB --------------------- #

def get_user_list(client, measurement: str) -> List[str]:
    """
    Get the list of all distinct user in the influxDB database with specified measurement data.

    Arguments
    ---------
    client: Client object
        influxDB client to connect to database.
    measurement: str
        Type of measurement of interest to extract user list.

    :return usr_list: list of all distinct user in database.
    """
    # Get list of all users
    influx_query = "SHOW TAG VALUES WITH KEY = \"user\""
    query_result = client.query(influx_query)
    user_values_dict = list(query_result.get_points(measurement=measurement))

    usr_list = []
    for elt in user_values_dict:
        usr_list.append(elt["value"])

    return list(set(usr_list))


def extract_raw_data_from_influxdb(client, measurement: str, user_id: str, start_time: str, end_time: str):
    """
    Extract raw data from influxDB.

    :param client: influxdb client
    :param measurement: measurement name
    :param user_id: id of user
    :param start_time: first timestamp for query
    :param end_time: last timestamp for query
    :return extracted_result_set: influxDB object containing extracted data
    """
    # Extract raw data from InfluxDB for D-day
    query = "SELECT * FROM {} WHERE \"user\" = '{}' and time > now() - {} and time < now() - {}".format(measurement,
                                                                                                        user_id,
                                                                                                        start_time,
                                                                                                        end_time)
    extracted_result_set = client.query(query)
    return extracted_result_set


# --------------------- FUNCTIONS TO COMPUTE TIME RANGE TO QUERY --------------------- #


def get_first_timestamp_to_compute_energy(user_id: str, client):
    """
    Retruns the first timestamp from which whe need to compute the energy.

    :param user_id: id of user
    :param client: influxdb client
    :return first_timestamp_to_compute_energy: timestamp
    """
    query = "SELECT last(\"energy_by_5s\") FROM MotionAccelerometer WHERE \"user\" = '{}'".format(user_id)
    extracted_data_result_set = client.query(query)
    last_energy_timestamp_for_user = list(extracted_data_result_set.get_points())

    if last_energy_timestamp_for_user:
        # Get last timestamp of energy data for user
        first_timestamp_to_compute_energy = last_energy_timestamp_for_user[0]["time"]
        print("Last energy timestamp in time series db: {}".format(first_timestamp_to_compute_energy))
    else:
        # Get first timestamp of Accelerometer data for user
        print("No energy data for user : {}".format(user_id))
        print("[Calculating energy from all MotionAccelerometer data]")
        query = "SELECT first(\"x_acm\") FROM MotionAccelerometer WHERE \"user\" = '{}'".format(user_id)
        extracted_data_result_set = client.query(query)
        if extracted_data_result_set:
            first_acm_timestamp_for_user = list(extracted_data_result_set.get_points())
            first_timestamp_to_compute_energy = first_acm_timestamp_for_user[0]["time"]

    first_timestamp_to_compute_energy = pd.to_datetime(first_timestamp_to_compute_energy, unit="ns")
    return first_timestamp_to_compute_energy


def get_time_difference_between_now_and_timestamp(timestamp):
    """
    Returns a timedelta in days, between current time and input timestamp

    :param timestamp: input timestamp
    :return days_time_delta: time difference with current time in days
    """
    current_timestamp = datetime.datetime.now()
    time_delta = current_timestamp - timestamp

    days_time_delta = time_delta.days
    return days_time_delta


# --------------------- FUNCTIONS TO COMPUTE ENERGY FROM ACM QUERY RESULT --------------------- #


def transform_acm_result_set_into_dataframe(result_set: str, tags: dict) -> pd.DataFrame:
    """
    Returns extracted accelerometer data from influxDB ResulSet object.

    :param result_set: InfluxDB Object containing raw data from query
    :param tags: influxdb tags from which to extract data. See influxdb python API for more informations.
    :return raw_acm_dataframe: pandas Dataframe containing Accelerometer Data.
    """
    raw_acm_data_list = list(result_set.get_points(measurement=ACCELEROMETER_MEASUREMENT_NAME, tags=tags))
    raw_acm_dataframe = pd.DataFrame(raw_acm_data_list)[["time", "x_acm", "y_acm", "z_acm"]]
    raw_acm_dataframe["time"] = pd.to_datetime(raw_acm_dataframe["time"])
    raw_acm_dataframe.index = raw_acm_dataframe["time"]
    return raw_acm_dataframe.dropna()


def create_energy_dataframe(acm_dataframe: pd.DataFrame, aggregation_count_threshold: int,
                            max_successive_time_diff: str, aggregation_time: str) -> pd.DataFrame:
    """
    Creates energy feature from raw accelerometer data and returns result as a dataframe.

    :param acm_dataframe: raw accelerometer dataframe
    :param aggregation_count_threshold: threshold above which we compute energy.
    :param max_successive_time_diff: maximum difference between successive timestamp below which we compute energy
    :param aggregation_time: time by which we aggreagte energy result.
    :return energy_dataframe: pandas dataframe object with 1 column, the energy, indexed by time
    """
    max_successive_time_diff_boolean_mask = acm_dataframe["time"].diff(periods=1) < max_successive_time_diff
    consecutive_differences_dataframe = acm_dataframe.diff(periods=1)[max_successive_time_diff_boolean_mask].drop(["time"], axis=1)

    squared_differences_dataframe = consecutive_differences_dataframe ** 2
    triaxial_sum_series = squared_differences_dataframe.apply(sum, axis=1)
    triaxial_sqrt_dataframe = triaxial_sum_series.apply(np.sqrt).to_frame()

    acm_dataframe_index = acm_dataframe[max_successive_time_diff_boolean_mask]["time"]
    triaxial_sqrt_dataframe.index = acm_dataframe_index
    triaxial_sqrt_dataframe.index.name = "timestamp"

    count_threshold_boolean_mask = triaxial_sqrt_dataframe.resample(aggregation_time, label="right").count() > aggregation_count_threshold
    energy_dataframe = triaxial_sqrt_dataframe.resample(aggregation_time, label="right").sum()
    energy_dataframe = energy_dataframe[count_threshold_boolean_mask].dropna()
    energy_dataframe = energy_dataframe.rename(columns={0: "energy_by_{}".format(aggregation_time)})

    return energy_dataframe


# --------------------- FUNCTIONS TO WRITE ENERGY DATA IN INFLUXDB --------------------- #


def chunk_and_write_dataframe(dataframe_to_write: pd.DataFrame, measurement: str,
                              user_id: str, df_client, batch_size: int = 5000) -> bool:
    """
    Split the input dataframe in chunk and write them sequentially for perfomance issues

    :param dataframe_to_write: pandas dataframe to write in influxDB
    :param measurement: measurement name of dataframe to write
    :param user_id: id of user
    :return:
    """
    # Chunk dataframe for time series db performance issues
    chunk_nb = math.ceil(len(dataframe_to_write) / batch_size)

    dataframe_chunk_list = np.array_split(dataframe_to_write, chunk_nb)
    # Write each chunk in time series db
    for chunk in dataframe_chunk_list:
        tags = {USER_PARAM_NAME: user_id}
        df_client.write_points(chunk, measurement=measurement, tags=tags, protocol="json")
    return True


def create_and_write_energy_for_user(user_id: str, client, df_client, accelerometer_measurement_name: str,
                                     five_sec_threshold: int, one_min_threshold: int, max_successive_time_diff: str,
                                     batch_size=5000):
    """
    Creates energy for user and write it in influxDB. It begins by extracting raw data from influxDB, process it
    to create the energy feature and then write batch of resulting feature data in influxDB.

    :param user_id: id of user
    :param client: influxdb client
    :param df_client: influxdb dataframe client
    :param accelerometer_measurement_name: measurement name of dataframe to write
    :param five_sec_threshold: threshold from which we consider that there are enough points to create energy \
    feature for each 5 seconds intervals.
    :param one_min_threshold: threshold from which we consider that there are enough points to create energy \
    feature for each one minute intervals.
    :param max_successive_time_diff:
    :param batch_size: number of points to set for each batch to write in influxDB. It splits pandas dataframe \
    in multiple dataframe of "batch_size" size, and write them sequentially for performance issues.
    :return:
    """
    print("-----------------------")
    print("[Creation of features] user {}".format(user_id))

    # # 1. Compute global time interval
    first_timestamp_to_compute_energy = get_first_timestamp_to_compute_energy(user_id, client=client)
    day_range_to_query = get_time_difference_between_now_and_timestamp(first_timestamp_to_compute_energy)

    # first_unix_timestamp_to_compute_energy = first_timestamp_to_compute_energy.replace(tzinfo=timezone.utc).timestamp()
    # print("UNIX TMSTP : {}".format(first_unix_timestamp_to_compute_energy))
    # print("DAY RANGE : {}".format(day_range_to_query))
    # print("UNIX TMSTP : {}".format(first_unix_timestamp_to_compute_energy))

    # # 3. Query raw Accelerometer data from time series db
    # start, end = str(int(first_unix_timestamp_to_compute_energy * 1000)) + "ms", str(day_range_to_query) + "d"
    # extracted_data_result_set = extract_raw_data_from_influxdb(CLIENT, ACCELEROMETER_MEASUREMENT_NAME,
    #                                                            user_id, start, end)
    #
    # # Transform InfluxDB ResultSet in pandas Dataframe
    # tags = {"user": user_id}
    # try:
    #     raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
    #     print(raw_acm_dataframe.shape)
    #
    #     # 4. Compute the energy feature
    #     five_sec_energy_dataframe = create_energy_dataframe(raw_acm_dataframe, threshold=225, aggregation_time="5S")
    #     #print("Five sec shape : {}".format(five_sec_energy_dataframe.shape))
    #
    #     # 5. Chunk resulting energy dataframe if necessary
    #     chunk_and_write_dataframe(five_sec_energy_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id,
    #                               DF_CLIENT, batch_size=5000)
    #
    #     # 4-bis. Compute the energy feature
    #     one_minute_energy_dataframe = create_energy_dataframe(raw_acm_dataframe,
    #                                                           threshold=2700, aggregation_time="1min")
    #     #print(one_minute_energy_dataframe.head())
    #     #print("One min shape : {}".format(one_minute_energy_dataframe.shape))
    #
    #     # 5-bis. Chunk resulting energy dataframe if necessary
    #     chunk_and_write_dataframe(one_minute_energy_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id,
    #                               DF_CLIENT, batch_size=5000)
    # except KeyError:
    #     pass
    #
    # print("DONE FOR 1ST PART")

    for day in reversed(range(day_range_to_query + 1)):
        # Extract raw data from InfluxDB for D-day
        start, end = str(day + 1) + "d", str(day) + "d"
        extracted_result_set = extract_raw_data_from_influxdb(client, accelerometer_measurement_name,
                                                              user_id, start, end)

        # Transform InfluxDB ResultSet in pandas Dataframe if resultset is not empty
        if extracted_result_set:
            tags = {"user": user_id}
            raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_result_set, tags)
            print("Raw dataframe shape: {}".format(raw_acm_dataframe.shape))
        else:
            continue

        # 4. Compute the energy feature
        five_sec_energy_dataframe = create_energy_dataframe(raw_acm_dataframe,
                                                            aggregation_count_threshold=five_sec_threshold,
                                                            max_successive_time_diff=max_successive_time_diff,
                                                            aggregation_time="5s")
        if not five_sec_energy_dataframe.empty:
            # 5. Chunk resulting energy dataframe (if necessary) and write in influxdb
            chunk_and_write_dataframe(five_sec_energy_dataframe, accelerometer_measurement_name, user_id,
                                      df_client, batch_size=batch_size)

        # 4-bis. Compute the energy feature
        one_minute_energy_dataframe = create_energy_dataframe(raw_acm_dataframe,
                                                              aggregation_count_threshold=one_min_threshold,
                                                              max_successive_time_diff=max_successive_time_diff,
                                                              aggregation_time="1min")
        if not one_minute_energy_dataframe.empty:
            # 5-bis. Chunk resulting energy dataframe (if necessary) and write in influxdb
            chunk_and_write_dataframe(one_minute_energy_dataframe, accelerometer_measurement_name, user_id,
                                      df_client, batch_size=batch_size)

        print("[Written process done]")


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.conf')

    # Useful Influx client constants
    influxdb_client_constants = config["Influxdb Client"]
    DB_NAME = influxdb_client_constants["database_name"]
    HOST = influxdb_client_constants["host"]
    PORT = int(influxdb_client_constants["port"])
    USER = influxdb_client_constants["user"]
    PASSWORD = influxdb_client_constants["password"]

    # MotionAccelerometer useful
    motion_acm_constants = config["Motion Accelerometer"]
    FIVE_SEC_THRESHOLD = motion_acm_constants["five_sec_threshold"]
    ONE_MIN_THRESHOLD = motion_acm_constants["one_min_threshold"]
    MAX_SUCCESSIVE_TIME_DIFF = motion_acm_constants["max_successive_time_diff"]

    # see InfluxDB Python API for more information
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD, database=DB_NAME)
    print("[Client created]")

    user_list = get_user_list(CLIENT, measurement=ACCELEROMETER_MEASUREMENT_NAME)

    for user_id in user_list:
        create_and_write_energy_for_user(user_id, CLIENT, DF_CLIENT, ACCELEROMETER_MEASUREMENT_NAME,
                                         FIVE_SEC_THRESHOLD, ONE_MIN_THRESHOLD, MAX_SUCCESSIVE_TIME_DIFF,
                                         batch_size=5000)


        # 2. Slice this global interval in as many 1 day intervals (from 00:00 to 23:59) as necessary
            #for each of those intervals:
                # 3. Query raw Accelerometer data from time series db
                # 4. Compute the energy feature
                # 5. Chunk resulting energy dataframe if necessary


        # write_energy_and_update_timestamp(user_id, timestamp_json_data, ENERGY_CHECKPOINT_BY_USER_JSON_FILENAME, CLIENT, DF_CLIENT)


   # TODO : delete
    # ENERGY_CHECKPOINT_BY_USER_JSON_FILENAME = "energy_checkpoint_by_user.json"
    # timestamp_json_data = extract_last_timestamp_for_users_from_checkpoint_file(ENERGY_CHECKPOINT_BY_USER_JSON_FILENAME, user_list)


# def extract_last_timestamp_for_users_from_checkpoint_file(file_name: str, user_list: list):
#     """
#     TODO
#     :param file_name:
#     :return:
#     """
#     try:
#         # TODO : refactor name variables
#         with open(file_name) as json_file:
#             json_timestamp_by_user = json.load(json_file)
#         for user in user_list:
#             if json_timestamp_by_user.get(user) is None:
#         # TODO : FIRST QUERY
#                 json_timestamp_by_user[user] = "2018-01-01T00:00:00"
#
#     except FileNotFoundError:
#         json_timestamp_by_user = {}
#         # Initialize timestamp
#         for user in user_list:
#             json_timestamp_by_user[user] = "2018-01-01T00:00:00"
#             # TODO : FIRST QUERY
#
#     return json_timestamp_by_user
#
#
# def update_user_timestamp(updated_timestamp, user, data, path_file):
#     """
#     TODO
#     :param updated_timestamp:
#     :param user:
#     :param data:
#     :param path_file:
#     :return:
#     """
#     data[user] = str(updated_timestamp)
#     with open(path_file, 'w') as processed_data_timestamp:
#         json.dump(timestamp_json_data, processed_data_timestamp)



# def write_user_energy_data(user_id: str, last_timestamp: str, client, df_client) -> bool:
#     """
#     TODO
#
#     :param user_id:
#     :param last_timestamp:
#     :return:
#     """
#     print("[Creation of features] user {}".format(user_id))
#
#     day_range_to_query, last_unix_timestamp = get_time_difference_between_now_and_timestamp(last_timestamp)
#
#     # Extract raw data from InfluxDB
#     start = str(int(last_unix_timestamp)) + "ms"
#     end = str(day_range_to_query) + "d"
     extracted_data_result_set = extract_raw_data_from_influxdb(client, ACCELEROMETER_MEASUREMENT_NAME,
#                                                                user_id, start, end)
#     # Transform InfluxDB ResultSet in pandas Dataframe
#     tags = {"user": user_id}
#     try:
#         raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
#         print(raw_acm_dataframe.shape)
#         # Create triaxial sum data points
#         triaxial_sum_dataframe = create_energy_dataframe(raw_acm_dataframe)
#         chunk_and_write_dataframe(triaxial_sum_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id, df_client)
#     except KeyError:
#         pass
#     for day in reversed(range(day_range_to_query)):
#         # Extract raw data from InfluxDB for D-day
#         start, end = str(day+1) + "d", str(day) + "d"
#         extracted_data_result_set = extract_raw_data_from_influxdb(client, ACCELEROMETER_MEASUREMENT_NAME,
#                                                                    user_id, start, end)
#
#         # Transform InfluxDB ResultSet in pandas Dataframe
#         tags = {"user": user_id}
#         try:
#             raw_acm_dataframe = transform_acm_result_set_into_dataframe(extracted_data_result_set, tags)
#             print(raw_acm_dataframe.shape)
#         except KeyError:
#             # If there is no data for D-day, skip processing
#             continue
#
#         # Create triaxial sum data points
#         triaxial_sum_dataframe = create_energy_dataframe(raw_acm_dataframe)
#
#         chunk_and_write_dataframe(triaxial_sum_dataframe, ACCELEROMETER_MEASUREMENT_NAME, user_id, df_client)
#         # TODO : deal with checkpoint before end
#     last_processed_data_timestamp = triaxial_sum_dataframe.tail(1).index.values[0]
#     print("[Writing process Done]")
#
#     return last_processed_data_timestamp


# def write_energy_and_update_timestamp(user_id: str, timestamp_json_data, json_data_file_path, client, df_client):
#     """
#
#     :param user_id:
#     :param timestamp_json_data:
#     :param json_data_file_path:
#     :param client:
#     :param df_client:
#     :return:
#     """
#     user_last_energy_timestamp = timestamp_json_data[user_id]
#
#     last_written_data_point_timestamp = write_user_energy_data(user_id, user_last_energy_timestamp, client, df_client)
#     # update_user_timestamp(last_written_data_point_timestamp, user_id, timestamp_json_data, json_data_file_path)
