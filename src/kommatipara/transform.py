from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType
from pyspark.sql.utils import AnalysisException
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List

from kommatipara.models import DatasetConfig
from kommatipara.utilities import handle_errors
from kommatipara.raw import save_to_file

@handle_errors
def read_table(spark: SparkSession, table_path: str, format: str, config_model: DatasetConfig) -> DataFrame:
    """
    Read the data file
    :param spark: SparkSession
    :param table_path: the path of the table file
    :param format: the format of the data
    :param config_model: the DatasetConfig model object

    :return: dataframe of the table
    """
    df = spark.read.format(format).load(f"{table_path}")

    if config_model.exclude:
        columns_to_drop = config_model.exclude
        df = df.drop(*columns_to_drop)
    
    return df

@handle_errors
def filter_table(df: DataFrame, config_model: DatasetConfig) -> DataFrame:
    """
    Filter the fields of the dataframe
    :param df: dataframe to be filtered
    :param config_model: the DataseConfig model object

    :return: dataframe filtered
    """
    if config_model.filters:
        for c in config_model.filters:
            values = config_model.filters[c]
            df.filter(F.col(c).isin(values))
    return df

@handle_errors
def join_table(df1: DataFrame, df2: DataFrame, columns_on_join: List[str]) -> DataFrame:
    """"
    Join the dataframes on the columns provided
    :param df1: dataframe to be joined
    :param df2: dataframe to be joined
    :param columns_on_join: columns to be joined on

    :return: joined dataframe
    """
    return df1.join(df2, on=columns_on_join)

@handle_errors
def rename_columns(df: DataFrame, columns_to_rename: Dict[str, str]) -> DataFrame:
    """
    Rename the columns to the provided names
    :param df: dataframe containing the columns to be renamed
    :param columns_to_rename: dictionary containing the mapping of the old column names 
     and new column names

    :return: dataframe with the new column names
    """
    for c in columns_to_rename:
        df = df.withColumnRenamed(c, columns_to_rename[c])

    return df

def output_data(spark: SparkSession, table_path1: str, table_path2: str, 
                config_model1: DatasetConfig, config_model2: DatasetConfig, 
                columns_on_join: List[str], columns_to_rename: Dict[str, str], 
                output_path: str, output_format: str, mode: str):
    """
    Transform the dataframes and output to file
    :param spark: SparkSession
    :param table_path1: the path of the table1 file
    :param table_path2: the path of the table2 file
    :param config_model1: the DatasetConfig model object of table1
    :param config_model2: the DatasetConfig model object of table2
    :param columns_on_join: columns to be joined on
    :param columns_to_rename: dictionary containing the mapping of the old column names 
     and new column names
    :param output_path: the path of the output file
    :param output_format: the format of the output file
    :param mode: the write mode 
    """
    # Read the tables
    df1 = read_table(spark, table_path1, output_format, config_model1)
    df2 = read_table(spark, table_path2, output_format, config_model2)

    # Filter df1
    df1 = filter_table(df1, config_model1)

    # Join the dataframes
    joined_df = join_table(df1, df2, columns_on_join)

    # Rename the columns
    renamed_df = rename_columns(joined_df, columns_to_rename)

    # Save the data
    save_to_file(renamed_df, output_path, output_format, mode)