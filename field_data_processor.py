import re
from data_ingestion import create_db_engine, query_data, read_from_web_CSV

from field_data_processor import FieldDataProcessor
import logging
from config import config_params

class FieldDataProcessor:
    """
    A class for processing field data.

    Args:
        config_params (dict): A dictionary containing configuration parameters for data processing.
        logging_level (str): The logging level for the class. Defaults to "INFO".

    Attributes:
        db_path (str): The path to the SQLite database.
        sql_query (str): The SQL query to retrieve data from the database.
        columns_to_rename (dict): A dictionary mapping column names to be renamed.
        values_to_rename (dict): A dictionary mapping values to be renamed.
        weather_csv_path (str): The path to the weather station CSV data.
        weather_mapping_csv (str): The path to the weather station mapping CSV data.
        df (DataFrame): The DataFrame to store processed data.
        engine: The database engine.

    Methods:
        initialize_logging(logging_level): Initializes logging for the class.
        ingest_sql_data(): Ingests data from an SQL database.
        rename_columns(): Renames columns in the DataFrame.
        apply_corrections(column_name='Crop_type', abs_column='Elevation'): Applies corrections to DataFrame columns.
        weather_station_mapping(): Maps weather station data to the main DataFrame.
        process(): Calls methods to process data in sequence.
    """

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes a FieldDataProcessor instance.

        Args:
            config_params (dict): A dictionary containing configuration parameters for data processing.
            logging_level (str, optional): The logging level for the class. Defaults to "INFO".
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_csv_path = config_params['weather_csv_path']
        self.weather_mapping_csv = config_params['weather_mapping_csv']
        self.initialize_logging(logging_level)
        self.df = None
        self.engine = None

    def initialize_logging(self, logging_level):
        """
        Initializes logging for the class.

        Args:
            logging_level (str): The logging level for the class.
        """
        logger_name = __name__ + ".FieldDataProcessor"
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

    def ingest_sql_data(self):
        """
        Ingests data from an SQL database.
        """
        self.engine = create_db_engine(self.db_path)
        self.df = config_params.query_data(self.engine, self.sql_query)
        self.logger.info("Sucessfully loaded data.")
        return self.df

    def rename_columns(self):
        """
        Renames columns in the DataFrame.
        """
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})
        self.logger.info(f"Swapped columns: {column1} with {column2}")

    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to DataFrame columns.

        Args:
            column_name (str, optional): The name of the column to apply corrections to. Defaults to 'Crop_type'.
            abs_column (str, optional): The name of the column to take the absolute value of. Defaults to 'Elevation'.
        """
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))

    def weather_station_mapping(self):
        """
        Maps weather station data to the main DataFrame.
        """
        self.df = self.df.merge(read_from_web_CSV(self.weather_map_data), on='Field_ID', how='outer')

    def process(self):
        """
        Processes data by calling methods in sequence.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()

# Instantiating the FieldDataProcessor class
field_processor = FieldDataProcessor(config_params)
field_processor.process()
field_df = field_processor.df