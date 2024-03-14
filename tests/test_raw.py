import unittest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from kommatipara.raw import read_from_file, save_to_file
from kommatipara.models import DatasetConfig

class TestRawFunctions(unittest.TestCase):
    def test_read_from_file(self):
        # Mocking the SparkSession
        spark = MagicMock(SparkSession)
        
        # Mocking other parameters
        path = "test.csv"
        format = "csv"
        sep = ","
        schema = StructType([StructField("id", IntegerType(), True)])
        
        # Mocking the return value of spark.read.load
        expected_df = MagicMock(DataFrame)
        spark.read.load.return_value = expected_df
        
        # Test when the file is read successfully
        self.assertEqual(read_from_file(spark, path, format, sep, schema), expected_df)

    def test_save_to_file(self):
        # Mocking the DataFrame
        df = MagicMock(DataFrame)
        
        # Mocking other parameters
        file_path = "test_file"
        format = "delta"
        mode = "overwrite"
        
        # Mocking the df.write.saveAsTable method
        df.write.save = MagicMock()
        
        # Test when saving to table succeeds
        save_to_file(df, file_path, format, mode)
        df.write.save.assert_called_once_with(file_path, format=format, mode=mode)
        

if __name__ == '__main__':
    unittest.main()
