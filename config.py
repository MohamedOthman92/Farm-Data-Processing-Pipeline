from field_data_processor import FieldDataProcessor
from weather_data_processor import WeatherDataProcessor

# Configuration parameters for data processing
config_params = {
    "sql_query": """
        SELECT *
        FROM geographic_features
        LEFT JOIN weather_features USING (Field_ID)
        LEFT JOIN soil_and_crop_features USING (Field_ID)
        LEFT JOIN farm_management_features USING (Field_ID)
        """,
    "db_path": "sqlite:///farm-survey.db",
    "columns_to_rename": {'Annual_yield': 'Crop_type', 'Crop_type': 'Annual_yield'},
    "values_to_rename": {'cassaval': 'cassava', 'wheatn': 'wheat', 'teaa': 'tea'},
    "weather_csv_path": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv",
    "weather_mapping_csv": "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"
}

def run_field_data_processing(config_params):
    """
    Process field data using the provided configuration parameters.

    Args:
        config_params (dict): A dictionary containing configuration parameters.

    Returns:
        pandas.DataFrame: Processed field data DataFrame.
    """
    field_processor = FieldDataProcessor(config_params)
    field_processor.process()
    field_df = field_processor.df
    return field_df

def run_weather_data_processing(config_params):
    """
    Process weather data using the provided configuration parameters.

    Args:
        config_params (dict): A dictionary containing configuration parameters.

    Returns:
        pandas.DataFrame: Processed weather data DataFrame.
    """
    weather_processor = WeatherDataProcessor(config_params)
    weather_processor.process()
    weather_df = weather_processor.weather_df
    return weather_df

"""
This script initializes and processes field data and weather data using the FieldDataProcessor and WeatherDataProcessor classes, respectively.
It loads configuration parameters from config_params, which include SQL queries, file paths, and renaming dictionaries.
After processing, it renames the 'Ave_temps' column in the field data DataFrame to 'Temperature' to match the weather data DataFrame.
"""
