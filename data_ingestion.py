"""
Module: data_ingestion

This module provides functions for data ingestion from SQLite databases and web-based CSV files.
"""

from sqlalchemy import create_engine, text
import logging
import pandas as pd

# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def create_db_engine(db_path):
    """
    Creates a database engine connection using the provided database path.

    Args:
        db_path (str): The path to the database file.

    Returns:
        sqlalchemy.engine.Engine: The created engine object.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        Exception: If there is an error creating the engine object.
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine  # Return the engine object if it all works well
    except ImportError: 
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise
    except Exception as e:
        logger.error(f"Failed to create database engine. Error: {e}")
        raise
    
def query_data(engine, sql_query):
    """
    Executes a SQL query against the provided database engine and returns the results as a Pandas DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine connection.
        sql_query (str): The SQL query to execute.

    Returns:
        pandas.DataFrame: The results of the SQL query.

    Raises:
        ValueError: If the query returns an empty DataFrame.
        Exception: If there is an error executing the query.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    Reads a CSV file from a web URL and returns the data as a Pandas DataFrame.

    Args:
        URL (str): The URL of the CSV file.

    Returns:
        pandas.DataFrame: The data from the CSV file.

    Raises:
        pd.errors.EmptyDataError: If the URL does not point to a valid CSV file.
        Exception: If there is an error reading the CSV file.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
