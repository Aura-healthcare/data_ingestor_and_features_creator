#!/usr/bin/env python
# coding: utf-8
"""This script provides methods to write JSON data into InfluxDB."""

import json
import logging
from logging.handlers import RotatingFileHandler
import math
import shutil
import datetime
import os
import glob
import numpy as np
import pandas as pd
from other_methods import create_df_with_unique_index
from rri_processing import (concat_rrinterval_files_into_single_dataframe,
                            create_corrected_timestamp_list)
from dataframe_converter import (convert_gyro_json_to_df,
                                 convert_acm_json_to_df)
from other_methods import (create_files_by_user_dict,
                           launch_retroactive_influxdb_cq)

# JSON field values
TYPE_PARAM_NAME = "type"
USER_PARAM_NAME = "user"
DEVICE_PARAM_NAME = "device_address"

# Create logger with custom formatter
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Handler file creation with 1 backup and max size of 1Mo
file_handler = RotatingFileHandler('airflow_dags.log', 'a', 1000000, 1)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s :: %(levelname)s :: %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Second handler which will write logs in the console
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)


# ---------------- WRITE PIPELINE METHODS ---------------- #

def write_file_to_influxdb(file: str, path_to_files: str, df_client) -> bool:
    """
    Function writing JSON file to influxDB.

    Arguments
    ---------
    file - JSON file to convert and write to InfluxDB
    path_to_data_test_directory - path for reading the JSON file
    get_tmstp - Option to extract all the timestamps of the JSON file

    Returns
    ---------
    write_success (Boolean) - Result of the write process
    """
    write_success = True

    # Open Json file
    try:
        with open(path_to_files + file) as json_file:
            json_data = json.load(json_file)
        # Get tags from file
        measurement = json_data[TYPE_PARAM_NAME]
        tags = {USER_PARAM_NAME: json_data[USER_PARAM_NAME],
                DEVICE_PARAM_NAME: json_data[DEVICE_PARAM_NAME]}
    except:
        logger.warning("Impossible to open file.")
        write_success = False
        return write_success

    try:
        # Convert json to pandas Dataframe
        if measurement == "MotionAccelerometer":
            data_to_write = convert_acm_json_to_df(json_data)
        else:
            data_to_write = convert_gyro_json_to_df(json_data)
    except:
        logger.warning("Impossible to convert file to Dataframe.")
        write_success = False
        return write_success

    # Checking if index of data is unique to avoid overwritten points in InfluxDB
    is_index_unique = data_to_write.index.is_unique
    if not is_index_unique:
        data_to_write = create_df_with_unique_index(data_to_write)

    # write to InfluxDB
    try:
        df_client.write_points(data_to_write, measurement=measurement, tags=tags, protocol="json")
    except:
        logger.warning("Impossible to write file to influxDB")
        write_success = False

    return write_success


def chunk_and_write_dataframe(dataframe_to_write: pd.DataFrame, measurement: str,
                              tags: dict, df_client, batch_size: int = 5000) -> bool:
    """
    TODO

    :param dataframe_to_write:
    :param measurement:
    :param user_id:
    :param df_client:
    :param batch_size:
    :return:
    """
    # Chunk dataframe for time series db performance issues
    chunk_nb = math.ceil(len(dataframe_to_write) / batch_size)
    logger.info("There are {} chunks to write.".format(chunk_nb))

    dataframe_chunk_list = np.array_split(dataframe_to_write, chunk_nb)
    # Write each chunk in time series db
    for chunk in dataframe_chunk_list:
        df_client.write_points(chunk, measurement=measurement, tags=tags, protocol="json")
    return True


def move_processed_file(file: str, write_success: bool, path_to_read_directory: str,
                        path_for_written_files: str, path_for_problem_files: str):
    """
    Function dealing with the JSON file once it is processed.

    Arguments
    ---------
    file - JSON file processed
    write_success - result of the writing process to influxDB
    path_to_read_directory - directory path where are JSON files
    path_for_written_files - directory where files writen in influxDB are moved
    path_for_problem_files - directory where files not correctly writen in influxDB are moved

    """
    if write_success:
        # move file when write is done in influxdb
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_written_files + file)
    else:
        shutil.move(src=path_to_read_directory + file,
                    dst=path_for_problem_files + file)


def rri_files_write_pipeline(user_rri_files: list, df_client, path_to_read_directory: str,
                             path_for_written_files: str, path_for_problems_files: str,
                             verbose: bool = True):
    """
    TODO

    :param user:
    :param user_rri_files:
    :param df_client:
    :param path_to_read_directory:
    :param path_for_written_files:
    :param path_for_problems_files:
    :param verbose:
    :return:
    """
    # concat multiple files of each user
    concatenated_dataframe = concat_rrinterval_files_into_single_dataframe(files_list=user_rri_files)

    # GET raw data count by min
    rri_count_by_min = concatenated_dataframe.resample("1min", label="right").count()
    rri_count_by_min.columns = ["rr_interval_count_by_min"]

    # Create new timestamp
    corrected_timestamp_list = create_corrected_timestamp_list(concatenated_dataframe)
    concatenated_dataframe.index = corrected_timestamp_list
    concatenated_dataframe.index.names = ["timestamp"]

    # Open Json file
    try:
        with open(user_rri_files[0]) as json_file:
            json_data = json.load(json_file)
        tags = {USER_PARAM_NAME: json_data[USER_PARAM_NAME],
                DEVICE_PARAM_NAME: json_data[DEVICE_PARAM_NAME]}
    except:
        logger.warning("Impossible to open file.")
        write_success = False

    # write raw RR-interval count by min to InfluxDB
    try:
        chunk_and_write_dataframe(rri_count_by_min, measurement="RrInterval",
                                  tags=tags, df_client=df_client, batch_size=5000)
    except:
        logger.warning("Impossible to write RR-interval raw data count by min to influxDB.")

    logger.info("DF SHAPE : {}".format(concatenated_dataframe.shape))
    # write processed rr_interval data to InfluxDB
    try:
        chunk_and_write_dataframe(concatenated_dataframe, measurement="RrInterval",
                                  tags=tags, df_client=df_client, batch_size=5000)
        write_success = True
    except:
        logger.warning("Impossible to write file to influxDB")
        write_success = False

    for json_file in user_rri_files:
        move_processed_file(json_file.split("/")[-1], write_success, path_to_read_directory,
                            path_for_written_files, path_for_problems_files)
        if verbose:
            file_processed_timestamp = str(datetime.datetime.now())
            log = "[" + file_processed_timestamp + "]" + " : " + json_file + " processed"
            logger.info(log)


def acm_gyro_write_pipeline(user_acm_files: list, df_client, path_to_read_directory: str,
                            path_for_written_files: str, path_for_problems_files: str,
                            verbose: bool = True):
    """
    TODO

    :param user_acm_files:
    :param df_client:
    :param path_to_read_directory:
    :param path_for_written_files:
    :param path_for_problems_files:
    :param verbose:
    :return:
    """
    for json_file in user_acm_files:
        write_success = write_file_to_influxdb(json_file.split("/")[-1], path_to_read_directory, df_client)
        move_processed_file(json_file.split("/")[-1], write_success, path_to_read_directory,
                            path_for_written_files, path_for_problems_files)
        if verbose:
            file_processed_timestamp = str(datetime.datetime.now())
            log = "[" + file_processed_timestamp + "] " + json_file + " file processed with success : {}".format(write_success)
            logger.info(log)


def execute_write_pipeline(measurement: str, path_to_read_directory: str, path_for_written_files: str,
                           path_for_problems_files: str, client, df_client, verbose: bool = True):
    """
    Process all gyroscope and accelerometer files in the read directory to write them to influxDB.

    Arguments
    ---------
    measurement - Measurement name for pipeline execution.
    path_to_read_directory - path from which we read JSON files to write into influxDB.
    path_for_written_files - path where we move correctly written files.
    path_for_problems_files - path where we move files for which write proccess failed.
    df_client - Dataframe InfluxDB Client
    verbose - Option to print some logs informations about process in console and logger.
    """

    # Get files list containing specified measurement data in directory
    files_list = glob.glob(path_to_read_directory + "*" + measurement + "*")
    files_list.sort()
    if verbose:
        logger.info("There are currently {} {} files to process.".format(len(files_list), measurement))

    # group and sort files by user
    sorted_files_dict = create_files_by_user_dict(files_list)

    # ---------- DELETE IN PRODUCTION MODE ---------- #
    # Creating directory for processed files
    if not os.path.exists(path_for_written_files):
        os.makedirs(path_for_written_files)
    if not os.path.exists(path_for_problems_files):
        os.makedirs(path_for_problems_files)

    for user in sorted_files_dict.keys():
        logger.info("{} data ingestor for user {}".format(measurement, user))
        user_acm_files = sorted_files_dict[user]

        if measurement == "RrInterval":
            rri_files_write_pipeline(user_acm_files, df_client, path_to_read_directory,
                                     path_for_written_files, path_for_problems_files, verbose)
        else:
            acm_gyro_write_pipeline(user_acm_files, df_client, path_to_read_directory,
                                    path_for_written_files, path_for_problems_files, verbose)

        # Get first timestamp to raise Flag to do retroactive CQ if necessary
        first_file_timestamp_for_user = pd.to_datetime(user_acm_files[0].split("_")[-1].split("T")[0])
        days_difference = (datetime.datetime.now() - first_file_timestamp_for_user).days

        if days_difference > 0:
            launch_retroactive_influxdb_cq(client, user, measurement, first_file_timestamp_for_user)
            logger.info("Retroactive {} Continuous Queries executed with success for user {}.".format(measurement, user))
        else:
            logger.info("No need to launch Retroactive continuous query because there are only today's data.")

