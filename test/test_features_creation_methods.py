#!/usr/bin/env python
# coding: utf-8

import math
import unittest
import pandas as pd
from features_creator.energy_feature_injector_methods import (create_energy_dataframe, get_user_list,
                                                              transform_acm_result_set_into_dataframe,
                                                              get_timestamps_for_query)


class FeaturesCreationMethodsTestCase(unittest.TestCase):

    def test_if_returned_timestamp_range_for_a_whole_day_from_midnight_to_midnight(self):
        expected = []

        pass

    def test_if_get_user_list_send_back_the_complete_list_of_user_in_the_database(self):
        # TODO ?
        # Mock
        # expected_user_list = []
        # timeseries_db_client = InfluxDBClient(host=HOST, port=PORT, username=USER,
        #                                       password=PASSWORD, database=DB_NAME)
        #
        # returned_user_list = get_user_list(timeseries_db_client)
        pass

    def test_if_transform_acm_result_set_into_dataframe_return_the_good_dataframe(self):
        # TODO ?
        # result_set_mock = [
        #     {'time': '2000-01-01T00:00:01.90Z',
        #      'device_address': 'test_device',
        #      'sensibility': '2G', 'squared_diff_sum': None,
        #      'user': 'test_user',
        #      'x_acm': 0.5, 'y_acm': 1, 'z_acm': -1},
        #     {'time': '2000-01-01T00:00:02Z',
        #      'device_address': 'test_device',
        #      'sensibility': '2G', 'squared_diff_sum': None,
        #      'user': 'test_user',
        #      'x_acm': 0.5, 'y_acm': 1, 'z_acm': -1}
        # ]
        #
        # with mock.patch('__main__.result_set.get_point') as MockClass:
        #     MockClass.return_value = result_set_mock
        #
        # result_set = {}
        # tags = {"user": "test_user"}
        # result = transform_acm_result_set_into_dataframe(result_set, tags=tags)
        # print(result)
        pass

    def test_if_create_energy_data_points_is_accurate(self):
        input_timestamp = ["2000-01-01 00:00:01.90", "2000-01-01 00:00:01.95", "2000-01-01 00:00:02",
                           "2000-01-01 00:00:10", "2000-01-01 00:00:10.05", "2000-01-01 00:00:10.1",
                           "2000-01-01 00:00:20", "2000-01-01 00:00:30"]
        input_data = {
            "time": pd.to_datetime(input_timestamp),
            "x_acm": pd.Series([1, 2, 1.5, -1.5, 0, 2, 0, 1]),
            "y_acm": pd.Series([1, 2, 1.5, -1.5, 0, 2, 0, 1]),
            "z_acm": pd.Series([1, 2, 1.5, -1.5, 0, 2, 0, 1])
        }
        input_dataframe = pd.DataFrame(input_data)
        input_dataframe.index.name = "time"

        output_timestamp = ["2000-01-01 00:00:01.95", "2000-01-01 00:00:02",
                            "2000-01-01 00:00:10.05", "2000-01-01 00:00:10.1"]
        ouput_data = {
            "squared_diff_sum": pd.Series([math.sqrt(3), math.sqrt(0.75), math.sqrt(6.75), math.sqrt(12)],
                                          index=pd.to_datetime(output_timestamp))
        }

        expected_output_dataframe = pd.DataFrame(ouput_data)
        expected_output_dataframe.index.name = "timestamp"
        method_result_dataframe = create_energy_dataframe(input_dataframe)
        print(method_result_dataframe)
        are_df_equals = expected_output_dataframe.equals(method_result_dataframe)
        self.assertTrue(are_df_equals)


if __name__ == '__main__':
    unittest.main()
