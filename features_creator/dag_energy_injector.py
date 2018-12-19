#!/usr/bin/env python
# coding: utf-8
"""This script defines Dags to inject data into influxDB via airflow."""

from datetime import datetime, timedelta
import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from energy_feature_injector_methods import (extract_user_timestamp_json,
                                             write_energy_and_update_timestamp,
                                             get_user_list)

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

# see InfluxDB Python API for more information
# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                        database=DB_NAME)

# Create influxDB dataframe client
DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
print("[Client created]")

MEASUREMENT = "MotionAccelerometer"
user_list = get_user_list(CLIENT, MEASUREMENT)

JSON_DATA_FILE_PATH = "processed_data_timestamp.json"
timestamp_json_data = extract_user_timestamp_json(JSON_DATA_FILE_PATH, user_list)

airflow_config = config["Airflow"]
default_args = {
    'owner': airflow_config["owner"],
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 2, 14, 5),
    'email': airflow_config["email"],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('energy_data_injector', default_args=default_args, schedule_interval="@hourly")

for user in user_list:
    write_energy_data = PythonOperator(task_id='write_energy_into_influxDB',
                                       python_callable=write_energy_and_update_timestamp,
                                       op_kwargs={"user_id": user,
                                                  "timestamp_json_data": timestamp_json_data,
                                                  "json_data_file_path": JSON_DATA_FILE_PATH,
                                                  "df_client": DF_CLIENT
                                                  },
                                       dag=dag)
    write_energy_data
