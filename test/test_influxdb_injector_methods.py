#!/usr/bin/env python
# coding: utf-8
"""This script tests different methods of the influxDB injector"""

import unittest
import pandas as pd
import numpy as np
from influxdb_raw_data_injector.influxdb_raw_data_injector import create_df_with_unique_index
from influxdb_raw_data_injector.influxdb_raw_data_injector import convert_acm_json_to_df
from influxdb_raw_data_injector.influxdb_raw_data_injector import convert_rri_json_to_df
from influxdb_raw_data_injector.influxdb_raw_data_injector import convert_gyro_json_to_df
from influxdb_raw_data_injector.influxdb_raw_data_injector import create_files_by_user_dict
from influxdb_raw_data_injector.influxdb_raw_data_injector import create_corrected_timestamp_list
from influxdb_raw_data_injector.influxdb_raw_data_injector import concat_rrinterval_files_into_single_dataframe


class InfluxdbInjectorMethodsTestCase(unittest.TestCase):

    def test_if_index_of_create_df_with_unique_index_is_unique(self):

        # Create dataframe
        dates = pd.date_range("20180924", periods=10, freq="S")
        df = pd.DataFrame(np.random.randint(500, 1000, 10), index=dates)
        # create 3 duplicated index
        df2 = pd.DataFrame(np.random.randint(500, 1000, 6), index=dates[:6])
        # create 2 triplicated index
        df3 = pd.DataFrame(np.random.randint(500, 1000, 3), index=dates[:3])
        result = pd.concat([df, df2, df3])

        # Compare index's length of original & resulting dataframe
        total_length_of_concatenated_df = len(result)
        length_new_index = len(set(create_df_with_unique_index(result).index))

        self.assertEqual(length_new_index, total_length_of_concatenated_df)

    def test_if_convert_acm_json_to_df_returns_a_dataframe(self):

        acm_json_test = {
            "user": "ef613034-3cab-4e90-a6f5-f98d36a8a9e1",
            "type": "MotionAccelerometer", "device_address": "EB:AD:8D:56:2A:73",
            "data": ["2018-07-18T12:33:46.596 0.51 -0.9 0.32 2G",
                     "2018-07-18T12:33:46.598 0.50 -0.92 0.31 2G",
                     "2018-07-18T12:33:46.600 0.508 -0.9 0.322 2G"]
        }

        index = pd.DatetimeIndex(["2018-07-18T12:33:46.596", "2018-07-18T12:33:46.598",
                                  "2018-07-18T12:33:46.600"])
        columns = ["x_acm", "y_acm", "z_acm", "sensibility"]
        values = [[0.51, -0.9, 0.32, "2G"], [0.50, -0.92, 0.31, "2G"], [0.508, -0.9, 0.322, "2G"]]
        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = "timestamp"

        # true if all lines & columns are equal
        result = ((convert_acm_json_to_df(acm_json_test) == df).all()).all()

        self.assertTrue(result)

    def test_if_convert_gyro_json_to_df_returns_a_dataframe(self):

        gyro_json_test = {
            "user": "ef613034-3cab-4e90-a6f5-f98d36a8a9e1",
            "type": "MotionGyroscope", "device_address": "EB:AD:8D:56:2A:73",
            "data": ["2018-07-18T12:33:46.596 0.51 -0.9 0.32",
                     "2018-07-18T12:33:46.598 0.50 -0.92 0.31",
                     "2018-07-18T12:33:46.600 0.508 -0.9 0.322"]
        }

        index = pd.DatetimeIndex(["2018-07-18T12:33:46.596", "2018-07-18T12:33:46.598",
                                  "2018-07-18T12:33:46.600"])
        columns = ["x_gyro", "y_gyro", "z_gyro"]
        values = [[0.51, -0.9, 0.32], [0.50, -0.92, 0.31], [0.508, -0.9, 0.322]]
        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = "timestamp"

        # true if all lines & columns are equal
        result = ((convert_gyro_json_to_df(gyro_json_test) == df).all()).all()

        self.assertTrue(result)

    def test_if_convert_rri_json_to_df_returns_a_dataframe(self):
        rri_json_test = {
            "user": "ef613034-3cab-4e90-a6f5-f98d36a8a9e1",
            "type": "MotionGyroscope", "device_address": "EB:AD:8D:56:2A:73",
            "data": ["2018-07-18T12:33:46.596 1000",
                     "2018-07-18T12:33:46.598 900",
                     "2018-07-18T12:33:46.600 950"]
        }

        index = pd.DatetimeIndex(["2018-07-18T12:33:46.596", "2018-07-18T12:33:46.598",
                                  "2018-07-18T12:33:46.600"])
        columns = ["RrInterval"]
        values = [[1000], [900], [950]]
        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = "timestamp"

        # true if all lines & columns are equal
        result = ((convert_rri_json_to_df(rri_json_test) == df).all()).all()

        self.assertTrue(result)

    def test_if_dict_contains_files_grouped_by_user_and_sorted(self):
        expected = {'7364575b': ['7364575b_RrInterval_2018-07-08T204618217.json',
                                 '7364575b_RrInterval_2018-07-10T214931799.json',
                                 '7364575b_RrInterval_2018-07-16T131919990.json'],
                    '8284fa83': ['8284fa83_RrInterval_2018-07-06T165511451.json',
                                 '8284fa83_RrInterval_2018-07-10T152624916.json'],
                    'acc276df': ['acc276df_RrInterval_2018-07-10T133525569.json',
                                 'acc276df_RrInterval_2018-07-11T144844897.json',
                                 'acc276df_RrInterval_2018-07-17T155933348.json'],
                    'ef613034': ['ef613034_RrInterval_2018-07-15T135401014.json',
                                 'ef613034_RrInterval_2018-07-16T144443744.json',
                                 'ef613034_RrInterval_2018-07-19T181033984.json',
                                 'ef613034_RrInterval_2018-07-20T154524633.json']}

        files_list = ['7364575b_RrInterval_2018-07-08T204618217.json',
                      '7364575b_RrInterval_2018-07-10T214931799.json',
                      '7364575b_RrInterval_2018-07-16T131919990.json',
                      '8284fa83_RrInterval_2018-07-06T165511451.json',
                      '8284fa83_RrInterval_2018-07-10T152624916.json',
                      'acc276df_RrInterval_2018-07-10T133525569.json',
                      'acc276df_RrInterval_2018-07-11T144844897.json',
                      'acc276df_RrInterval_2018-07-17T155933348.json',
                      'ef613034_RrInterval_2018-07-15T135401014.json',
                      'ef613034_RrInterval_2018-07-16T144443744.json',
                      'ef613034_RrInterval_2018-07-19T181033984.json',
                      'ef613034_RrInterval_2018-07-20T154524633.json']

        result = create_files_by_user_dict(files_list)
        self.maxDiff = None
        self.assertDictEqual(expected, result)

    def test_if_files_content_are_concatenated_as_a_single_dataframe(self):
        # TODO
        # columns = ["RrInterval"]
        # index = ["2018-07-16 13:04:00", "2018-07-16 13:04:01", "2018-07-16 13:04:02",
        #          "2018-07-16 13:04:03", "2018-07-16 13:04:06"]
        # rri_values = [760, 860, 767, 759, 748]
        # expected = pd.DataFrame(data=rri_values, columns=columns, index=index)
        #
        # files_list = ["test_file1.json", "test_file2.json"]
        # returned = concat_files_into_dataframe(files_list)
        pass

    def test_if_created_timestamp_by_create_corrected_timestamp_list_is_correct(self):
        timestamp_list = ['2017-12-31 23:59:51', '2017-12-31 23:59:52',
                          '2017-12-31 23:59:53', '2017-12-31 23:59:54',
                          '2017-12-31 23:59:57.1', '2017-12-31 23:59:58',
                          '2017-12-31 23:59:59', '2018-01-01 00:00:00',
                          '2018-01-01 00:00:08.2', '2018-01-01 00:00:19.1',
                          '2018-01-01 00:00:20.05', '2018-01-01 00:00:21.3',
                          '2018-01-01 00:00:21.3', '2018-01-01 00:00:22.2',
                          '2018-01-01 00:00:30', '2018-01-01 00:00:40']

        rri_list = [950, 1000, 1010, 980, 1100, 1000, 1000, 1200, 900, 1050, 1100, 400, 450, 900, 1000, 1000]
        test_df = pd.DataFrame(data=rri_list, index=pd.to_datetime(timestamp_list), columns=["RrInterval"])
        returned = create_corrected_timestamp_list(test_df)

        expected_timestamp_list = pd.to_datetime(['2017-12-31 23:59:51', '2017-12-31 23:59:52',
                                                  '2017-12-31 23:59:53.01', '2017-12-31 23:59:53.990',
                                                  '2017-12-31 23:59:57.1', '2017-12-31 23:59:58.1',
                                                  '2017-12-31 23:59:59.1', '2018-01-01 00:00:00.3',
                                                  '2018-01-01 00:00:08.2', '2018-01-01 00:00:19.1',
                                                  '2018-01-01 00:00:20.2', '2018-01-01 00:00:20.6',
                                                  '2018-01-01 00:00:21.05', '2018-01-01 00:00:21.95',
                                                  '2018-01-01 00:00:30', '2018-01-01 00:00:40'])

        result = ((returned == expected_timestamp_list).all()).all()
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
