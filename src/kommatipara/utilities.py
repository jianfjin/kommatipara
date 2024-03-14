from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType
from pyspark.sql.utils import AnalysisException
import logging
from typing import Dict, List
from functools import wraps
from pydantic import ValidationError

from kommatipara.models import DatasetConfig

def load_config_model(config: Dict) -> DatasetConfig:
    """
    Load and validate the DatasetCofig model.
    :param config: the dictionary of config

    :return: DatasetConfig object
    """
    try:
        return DatasetConfig(**config)
    except ValidationError as e:
        logging.info(e)

def create_schema(config_model: DatasetConfig) -> StructType:
    """
    Map the data types of the fields in the provided schema.
    :param config: The configuration of the fields.

    :return: StrucType of the fields
    """
    mapper = {
        "int": IntegerType(),
        "long": LongType(),
        "str": StringType()
    }
    columns = config_model.schema

    try:
        schema = StructType([StructField(c, mapper.get(columns[c], StringType()), True) for c in columns])
        logging.info("Schema created successfully.")
        return schema
    except KeyError as e:
        logging.error(f"The key is not found: {e}")

def handle_errors(func):
    """
    Handle the spark errors.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except AnalysisException as e:
            logging.error(e)
    return wrapper