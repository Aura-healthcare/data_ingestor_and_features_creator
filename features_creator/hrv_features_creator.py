#!/usr/bin/env python
# coding: utf-8

# from hrvanalysis.extract_features import (get_time_domain_features,
#                                           get_frequency_domain_features,
#                                           get_sampen, get_csi_cvi_features,
#                                           get_geometrical_features,
#                                           get_poincare_plot_features)
# from hrvanalysis import remove_outliers
# from hrvanalysis import interpolate_nan_values
# from hrvanalysis import remove_ectopic_beats


# if __name__ == "__main__":
#
#     # Useful Influx client constants
#     DB_NAME = "physio_signals"
#     HOST = "localhost"
#     PORT = 8086
#     USER = "root"
#     PASSWORD = "root"
#
#     # see InfluxDB Python API for more information
#     # https://influxdb-python.readthedocs.io/en/latest/api-documentation.html
#     CLIENT = InfluxDBClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
#                             database=DB_NAME)
#
#     # Create influxDB dataframe client
#     DF_CLIENT = DataFrameClient(host=HOST, port=PORT, username=USER, password=PASSWORD,
#                                 database=DB_NAME)
#     print("[Client created]")
#
#     # Get list of all users
#     user_list = get_user_list(CLIENT, "RrInterval")
#
#     # Convert in Dag Loop => parallel job
#     RrIntervals_dict = {}
#
#     for user in user_list:
#
#         print("--------------------------------------")
#         print("[Creation of features] user {}".format(user))
#
#         # Rr Intervals pipeline
#         # Extract raw data from InfluxDB (for N last days)
#         query = "SELECT * FROM RrInterval WHERE \"user\" = '" + user + "' and time > now() - 100d"
#
#         # Transform data in a pandas Dataframe
#         result_query = CLIENT.query(query)
#         RrIntervals_dict[user] = list(result_query.get_points(measurement="RrInterval", tags={"user": user}))
#         rr_interval_dataframe = pd.DataFrame(RrIntervals_dict[user])[["time", "RrInterval"]].set_index("time")
#
#         # Remove outliers from raw data
#         cleaned_rrintervals = clean_outlier(rr_intervals=rr_interval_dataframe["RrInterval"].values.tolist(),
#                                             low_rri=250, high_rri=2000)
#         # Interpolate cleaned RrInterval
#         interpolated_rrintervals = interpolate_nan_values(cleaned_rrintervals, method="linear")
#
#         # KAMATH
#         # Clean ectopics beats from Rr Intervals
#         kamath_nn_intervals = clean_ectopic_beats(interpolated_rrintervals, method="Kamath")
#         # Interpolate NnIntervals
#         interpolated_nn_intervals = interpolate_nan_values(kamath_nn_intervals, method="linear")
#
#         # Resampling and rolling window for features calculation
#         pass
#         # Test if sample is valid, then calculate features
#         pass
#
#         # Map function on samples if they are valid
#         # Time domain features
#         time_features = get_time_domain_features(interpolated_nn_intervals)
#         geometrical_features = get_geometrical_features(interpolated_nn_intervals)
#
#         # Frequency domain features
#         freq_features = get_frequency_domain_features(interpolated_nn_intervals)
#
#         # Non Linear domain features
#         sampen_features = get_sampen(interpolated_nn_intervals)
#         csi_cvi_features = get_csi_cvi_features(interpolated_nn_intervals)
#         poincare_plot_features = get_poincare_plot_features(interpolated_nn_intervals)
#
#         print_features = False
#         if print_features:
#             print(time_features)
#             print(geometrical_features)
#             print(freq_features)
#             print(sampen_features)
#             print(csi_cvi_features)
#             print(poincare_plot_features)
