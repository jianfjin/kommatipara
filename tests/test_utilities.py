import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from kommatipara.utilities import create_schema, load_config_model
from kommatipara.models import DatasetConfig

class TestUtilities(unittest.TestCase):
    def test_load_valid_config(self):
        # Test loading a valid configuration
        config = {
            "name": "dataset_one",
            "format": "csv",
            "path": "dataset_one.csv",
            "sep": ",",
            "schema": {
                "id": "int",
                "first_name": "str",
                "last_name": "str",
                "email": "str",
                "country": "str"
            },
            "filters": {
                "country": ["United Kingdom", "Netherlands"]
            },
            "exclude": ["email"],
            "rename": {"id": "client_identifier"}
        }
        expected_model = DatasetConfig(
            name="dataset_one",
            format="csv",
            path="dataset_one.csv",
            sep=",",
            schema={
                "id": "int",
                "first_name": "str",
                "last_name": "str",
                "email": "str",
                "country": "str"
            },
            filters={"country": ["United Kingdom", "Netherlands"]},
            exclude=["email"],
            rename={"id": "client_identifier"}
        )
        self.assertEqual(load_config_model(config), expected_model)

    def test_create_schema(self):
        # Mocking the config_model parameter
        config_model = DatasetConfig(**{
            "name": "dataset_one",
            "format": "csv",
            "path": "dataset_one.csv",
            "sep": ",",
            "schema": {
                "id": "int",
                "first_name": "str",
                "last_name": "str",
                "email": "str",
                "country": "str"
            },
            "filters": {
                "country": ["United Kingdom", "Netherlands"]
            },
            "exclude": ["email"]
        })
        
        # Test when all keys are valid
        expected_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
        ])
        self.assertEqual(create_schema(config_model), expected_schema)