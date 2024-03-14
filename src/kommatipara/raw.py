from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType
from pyspark.sql.utils import AnalysisException
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List

from kommatipara.models import DatasetConfig
from kommatipara.utilities import create_schema, handle_errors

@handle_errors
def read_from_file(spark: SparkSession, path:str, format: str, sep: str, schema: StructType)-> DataFrame:
    """
    Read a file by Spark.
    :param spark: SparkSession
    :param path: the file path
    :param format: the file format
    :param sep: the delimiter of the file
    :param schema: the data types of the fields

    :return: dataframe of the file content
    """ 
    return spark.read.load(path=path, format=format, header=True, sep=sep, schema=schema)

@handle_errors
def save_to_file(df: DataFrame, output_path: str, format: str, mode: str):
    """
    Save the dataframe to file
    :param df: dataframe
    :param output_path: the path of the output file 
    """
    df.write.save(output_path, format=format, mode=mode)

def load_raw_data(spark: SparkSession, source_location: str, datalake_dir: str, 
                  db_name: str, config_model: DatasetConfig, schema: StructType,
                  output_format: str, mode: str):
    """
    Write the raw data files to data lake
    :param spark: SparkSession
    :param source_location: the location of the source file
    :param datalake_dir: the directory of the data lake
    :param db_name: the name of the data lake
    :param config_model: the DatasetConfig model object
    :param schema: the data types of the fields
    :param output_format: the format of the output file
    :param mode: the write mode 
    """
    # Set the input path
    # file = f'{source_location}/{config_model.path}'

    # Set the output path
    output_path = f'{datalake_dir}/{db_name}.db/{config_model.name}'

    # Read data
    df = read_from_file(spark, source_location, config_model.format, config_model.sep, schema)
    
    # Save data
    save_to_file(df, output_path, output_format, mode)