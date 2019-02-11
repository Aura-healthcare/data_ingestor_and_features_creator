#!/usr/bin/env python
# coding: utf-8
"""This script defines main for the raw data ingestor."""

import configparser
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from write_pipeline_methods import execute_write_pipeline


if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('config.conf')

    files_processing_paths = config["Paths"]
    PATH_TO_READ_DIRECTORY = files_processing_paths["read_directory"]
    PATH_FOR_WRITTEN_FILES = files_processing_paths["success_files_directory"]
    PATH_FOR_PROBLEMS_FILES = files_processing_paths["failed_files_directory"]

    influxdb_client_constants = config["Influxdb Client"]
    DB_NAME = influxdb_client_constants["database_name"]
    HOST = influxdb_client_constants["host"]
    PORT = int(influxdb_client_constants["port"])
    USER = influxdb_client_constants["user"]
    PASSWORD = influxdb_client_constants["password"]

    # Create influxDB clients - see InfluxDB Python API for more information
    # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
    CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
    DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                                database=DB_NAME)

    # Create database
    # CLIENT.create_database(DB_NAME)

    # -------- Write pipeline -------- #
    for measurement in ["RrInterval", "MotionAccelerometer", "MotionGyroscope"]:
        execute_write_pipeline(measurement, PATH_TO_READ_DIRECTORY, PATH_FOR_WRITTEN_FILES,
                               PATH_FOR_PROBLEMS_FILES, CLIENT, DF_CLIENT, True)
