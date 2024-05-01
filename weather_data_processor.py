import re
import logging
from data_ingestion import read_from_web_CSV

class WeatherDataProcessor:
    """
    A class for processing weather station data.

    Args:
        config_params (dict): A dictionary containing configuration parameters for data processing.
        logging_level (str): The logging level for the class. Defaults to "INFO".

    Attributes:
        weather_station_data (str): The path to the weather station CSV data.
        patterns (dict): A dictionary containing regular expression patterns for extracting measurements from messages.
        weather_df (DataFrame): The DataFrame to store weather station data.
        logger: The logger object for logging messages.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes a WeatherDataProcessor instance.

        Args:
            config_params (dict): A dictionary containing configuration parameters for data processing.
            logging_level (str, optional): The logging level for the class. Defaults to "INFO".
        """
        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None
        self.initialize_logging(logging_level)
        
    def initialize_logging(self, logging_level):
        """
        Initializes logging for the class.

        Args:
            logging_level (str): The logging level for the class.
        """
        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False
        
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO
        
        self.logger.setLevel(log_level)
        
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        
    def weather_station_mapping(self):
        """
        Loads weather station data from a web CSV file.
        """
        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.") 
    
    def extract_measurement(self, message):
        """
        Extracts measurements from a message using regular expressions.

        Args:
            message (str): The message containing measurements.

        Returns:
            tuple: A tuple containing the measurement key and value.
        """
        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))
        self.logger.debug("No measurement match found.")
        return None, None
    
    def process_messages(self):
        """
        Processes messages in the weather station data to extract measurements.
        """
        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
        return self.weather_df
    
    def calculate_means(self):
        """
        Calculates the mean values of measurements.

        Returns:
            DataFrame: A DataFrame containing mean values of measurements.
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    
    def process(self):
        """
        Processes weather station data.
        """
        self.weather_station_mapping()
        self.process_messages()
        self.logger.info("Data processing completed.")
