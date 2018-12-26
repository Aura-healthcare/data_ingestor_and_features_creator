#!/usr/bin/env python
# coding: utf-8
"""This script defines Dags to inject data into influxDB via airflow."""

from datetime import datetime, timedelta
import configparser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from influxdb import InfluxDBClient
from influxdb import DataFrameClient
from energy_feature_injector_methods import (create_and_write_energy_for_users, get_user_list)

config = configparser.ConfigParser()
config.read('config.conf')

# InfluxDB useful configuration information
influxdb_client_constants = config["Influxdb Client"]
DB_NAME = influxdb_client_constants["database_name"]
HOST = influxdb_client_constants["host"]
PORT = int(influxdb_client_constants["port"])
USER = influxdb_client_constants["user"]
PASSWORD = influxdb_client_constants["password"]

# MotionAccelerometer useful configuration information
motion_acm_constants = config["Motion Accelerometer"]
FIVE_SEC_THRESHOLD = motion_acm_constants["five_sec_threshold"]
ONE_MIN_THRESHOLD = motion_acm_constants["one_min_threshold"]
MAX_SUCCESSIVE_TIME_DIFF = motion_acm_constants["max_successive_time_diff"]
ACCELEROMETER_MEASUREMENT_NAME = motion_acm_constants["measurement_name"]

# see InfluxDB Python API for more information
# https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                        database=DB_NAME)
DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
                            database=DB_NAME)
print("[Client created]")

user_list = get_user_list(CLIENT, ACCELEROMETER_MEASUREMENT_NAME)
print("There are {} users with Accelerometer Data.".format(len(user_list)))

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

dag = DAG('energy_data_injector', default_args=default_args, schedule_interval="@daily")

write_energy_data = PythonOperator(task_id='create_and_write_energy_for_users',
                                   python_callable=create_and_write_energy_for_users,
                                   op_kwargs={"user_list": user_list,
                                              "client": CLIENT,
                                              "df_client": DF_CLIENT,
                                              "accelerometer_measurement_name": ACCELEROMETER_MEASUREMENT_NAME,
                                              "five_sec_threshold": FIVE_SEC_THRESHOLD,
                                              "one_min_threshold": ONE_MIN_THRESHOLD,
                                              "max_successive_time_diff": MAX_SUCCESSIVE_TIME_DIFF,
                                              "batch_size": 5000
                                              },
                                   dag=dag)

write_energy_data