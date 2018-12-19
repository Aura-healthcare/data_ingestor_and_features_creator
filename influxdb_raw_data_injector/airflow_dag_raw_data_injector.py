#!/usr/bin/env python
# coding: utf-8
"""This script defines Dags to inject data into influxDB via airflow."""

from datetime import datetime, timedelta
import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from influxdb_raw_data_injector import (execute_acm_gyro_files_write_pipeline,
                                        execute_rri_files_write_pipeline)

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
DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
print("[Client created]")

# Create database
#CLIENT.create_database(DB_NAME)

airflow_config = config["Airflow"]
default_args = {
    'owner': airflow_config["owner"],
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 11, 12, 30),
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

dag = DAG('raw_data_injector', default_args=default_args, schedule_interval="@hourly")

write_rri_data = PythonOperator(task_id='write_rri_data_into_influxDB',
                                python_callable=execute_rri_files_write_pipeline,
                                op_kwargs={"path_to_read_directory": PATH_TO_READ_DIRECTORY,
                                           "path_for_written_files": PATH_FOR_WRITTEN_FILES,
                                           "path_for_problems_files": PATH_FOR_PROBLEMS_FILES,
                                           "df_client": DF_CLIENT,
                                           "verbose": True},
                                dag=dag)

write_acm_gyro_data = PythonOperator(task_id='write_acm_gyro_data_into_influxDB',
                                     python_callable=execute_acm_gyro_files_write_pipeline,
                                     op_kwargs={"path_to_read_directory": PATH_TO_READ_DIRECTORY,
                                                "path_for_written_files": PATH_FOR_WRITTEN_FILES,
                                                "path_for_problems_files": PATH_FOR_PROBLEMS_FILES,
                                                "df_client": DF_CLIENT,
                                                "verbose": True},
                                     dag=dag)

write_acm_gyro_data.set_upstream(write_rri_data)
