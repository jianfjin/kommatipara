import unittest
from unittest.mock import MagicMock
from kommatipara.transform import read_table, filter_table, join_table, rename_columns, DatasetConfig, handle_errors
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.utils import AnalysisException
import pytest
from mockito import when

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest_spark_testing") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture(scope="session")
def mock_table(spark_session):
    data = [(1, "Alice", 30), (2, "Bob", 25)]
    columns = ["id", "name", "age"]
    return spark_session.createDataFrame(data, columns)

@pytest.mark.usefixtures("spark_session", "mock_table")
class TestFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("unit_test") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @handle_errors
    def dummy_read_function(self, spark: SparkSession, table_path: str, format: str, config_model: DatasetConfig) -> DataFrame:
        return self.spark.createDataFrame([(1, "Alice", 30), (2, "Bob", 25)], ["id", "name", "age"])
    
    @pytest.mark.skip(reason="the hadoop config is not correct")
    def test_read_table(self):
        # Mock DatasetConfig object
        config_model = DatasetConfig(name="dummy", format="csv", path="/dummy/path", sep=",", schema={"id": "int", "name": "str", "age": "int"})
        
        when(self.spark.read).load(...).thenReturn(mock_table)
        # Test when config_model.exclude is not None
        config_model.exclude = ["age"]
        result_df = read_table(self.spark, "/dummy/path", "csv", config_model.exclude)
        self.assertEqual(result_df.columns, ["id", "name"])

        # Test when config_model.exclude is None
        config_model.exclude = None
        result_df = read_table(self.spark, "/dummy/path", "csv", config_model.exclude)
        self.assertEqual(result_df.columns, ["id", "name", "age"])

    @pytest.mark.skip(reason="the hadoop config is not correct")
    def test_filter_table(self):
        # Mock DataFrame
        df = self.spark.createDataFrame([(1, "Alice", "USA"), (2, "Bob", "Canada")], ["id", "name", "country"])
        
        # Mock DatasetConfig object
        config_model = DatasetConfig(name="dummy", format="csv", path="/dummy/path", sep=",", schema={"id": "int", "name": "str", "country": "str"})

        # Test when config_model.filters is not None
        config_model.filters = {"country": ["USA"]}
        result_df = filter_table(df, config_model.filters)
        self.assertEqual(result_df.count(), 1)

        # Test when config_model.filters is None
        config_model.filters = None
        result_df = filter_table(df, config_model)
        self.assertEqual(result_df.count(), 2)

    def test_join_table(self):
        # Mock DataFrames
        df1 = self.spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        df2 = self.spark.createDataFrame([(1, "USA"), (2, "Canada")], ["id", "country"])

        # Test join operation
        result_df = join_table(df1, df2, ["id"])
        self.assertEqual(result_df.columns, ["id", "name", "country"])

    def test_rename_columns(self):
        # Mock DataFrame
        df = self.spark.createDataFrame([(1, "Alice", "USA"), (2, "Bob", "Canada")], ["id", "name", "country"])

        # Test renaming columns
        result_df = rename_columns(df, {"name": "full_name", "country": "location"})
        self.assertEqual(result_df.columns, ["id", "full_name", "location"])

if __name__ == '__main__':
    unittest.main()
