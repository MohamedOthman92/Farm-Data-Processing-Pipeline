import pandas as pd
from scipy.stats import ttest_ind
import numpy as np

# Functions for filtering data
def filter_field_data(df, station_id, measurement):
    """
    Filter field data based on a specific weather station ID and measurement.

    Args:
        df (DataFrame): The DataFrame containing field data.
        station_id (int): The ID of the weather station to filter data for.
        measurement (str): The measurement to filter, such as temperature, humidity, etc.

    Returns:
        Series: A Series containing the filtered data for the specified weather station and measurement.
    """
    filtered_data = df[(df['Weather_station'] == station_id)][measurement]
    return filtered_data

def filter_weather_data(df, station_id, measurement):
    """
    Filter weather data based on a specific weather station ID and measurement.

    Args:
        df (DataFrame): The DataFrame containing weather data.
        station_id (int): The ID of the weather station to filter data for.
        measurement (str): The measurement to filter, such as temperature, humidity, etc.

    Returns:
        Series: A Series containing the filtered data for the specified weather station and measurement.
    """
    filtered_data = df[(df['Weather_station_ID'] == station_id) & (df['Measurement'] == measurement)]['Value']
    return filtered_data

# Function to perform t-test
def run_ttest(data1, data2):
    """
    Perform a t-test for two independent samples.

    Args:
        data1 (array-like): First set of data.
        data2 (array-like): Second set of data.

    Returns:
        tuple: A tuple containing the t-statistic and p-value.
    """
    t_stat, p_val = ttest_ind(data1, data2)
    return t_stat, p_val

# Function to print t-test results
def print_ttest_results(station_id, measurement, p_val, alpha):
    """
    Print the results of a t-test.

    Args:
        station_id (int): The ID of the weather station.
        measurement (str): The measurement being tested.
        p_val (float): The p-value of the t-test.
        alpha (float): The significance level.
    """
    print(f"T-test results for station {station_id} and measurement '{measurement}':")
    print(f"  p-value: {p_val:.5f}")
    if p_val < alpha:
        print("  Null hypothesis rejected: There is a significant difference.")
    else:
        print("  Null hypothesis not rejected: There is no significant difference.")

# Function to perform hypothesis testing for multiple measurements
def hypothesis_results(field_df, weather_df, measurements_to_compare, alpha):
    """
    Perform hypothesis testing for multiple measurements.

    Args:
        field_df (DataFrame): DataFrame containing field data.
        weather_df (DataFrame): DataFrame containing weather data.
        measurements_to_compare (list): List of measurements to compare.
        alpha (float): The significance level.
    """
    print("Hypothesis Testing Results:")
    for measurement in measurements_to_compare:
        field_values = filter_field_data(field_df, station_id, measurement)
        weather_values = filter_weather_data(weather_df, station_id, measurement)
        t_stat, p_val = run_ttest(field_values, weather_values)
        print_ttest_results(station_id, measurement, p_val, alpha)

# Main function
def main():
    # Load your data (replace file paths with actual file paths)
    field_df = pd.read_csv('field_data.csv')
    weather_df = pd.read_csv('weather_data.csv')

    # Set input parameters
    station_id = 0
    alpha = 0.05
    measurements_to_compare = ['Temperature', 'Rainfall', 'Pollution_level']

    # Example usage
    print("Example 1:")
    measurement = 'Temperature'
    field_values = filter_field_data(field_df, station_id, measurement)
    weather_values = filter_weather_data(weather_df, station_id, measurement)
    t_stat, p_val = run_ttest(field_values, weather_values)
    print_ttest_results(station_id, measurement, p_val, alpha)

    print("\nExample 2:")
    hypothesis_results(field_df, weather_df, measurements_to_compare, alpha)

if __name__ == "__main__":
    main()
