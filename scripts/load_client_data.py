from pyspark.sql import SparkSession
from delta import *
import json
from pathlib import Path

import logging
from logging.handlers import RotatingFileHandler

from kommatipara.models import DatasetConfig
from kommatipara.raw import load_raw_data
from kommatipara.transform import output_data
from kommatipara.utilities import create_schema, load_config_model

# Configure logger
logger = logging.getLogger("etl_demo")
logger.setLevel(logging.INFO)

# Create rotating file handler
log_file = "etl_demo.log"
handler = RotatingFileHandler(log_file, maxBytes=1000000, backupCount=5)  # Adjust parameters as needed
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Set the paths
root_path = str(Path(__file__).parent.parent.resolve())
source_path = str(Path(__file__).parent.resolve())
config_path = f'{source_path}/resources/config.json'
source_location = f'file:///{source_path}/resources'
datalake_dir = f'file:///{root_path}/datalake'
db_name = 'client_data'
storage_location = f'file:///{root_path}/{db_name}'
output_format = 'delta'
mode = 'overwrite'

# Load the config from json
try:
    with open(config_path) as f:
        configs = json.load(f)
        logger.info("Config loaded successfully.")
except FileNotFoundError as e:
    logger.error(e)

# create the schemas for the tables
config_model1 = load_config_model(configs[0])
schema1 = create_schema(config_model1)
logger.info("Schema for dataset one created successfully.")

config_model2 = load_config_model(configs[1])
schema2 = create_schema(config_model2)
logger.info("Schema for dataset two created successfully.")

builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", datalake_dir)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
logger.info("Spark session created successfully.")

# Load the raw data to delta lake
file1 = f'{source_location}/{config_model1.path}'
output_path1 = f'{datalake_dir}/{db_name}.db/{config_model1.name}'
load_raw_data(spark, file1, datalake_dir, db_name, config_model1, schema1, output_format, mode)
logger.info("Dataset one written to data lake successfully.")

file2 = f'{source_location}/{config_model2.path}'
output_path2 = f'{datalake_dir}/{db_name}.db/{config_model2.name}'
load_raw_data(spark, file2, datalake_dir, db_name, config_model2, schema2, output_format, mode)
logger.info("Dataset two written to data lake successfully.")

# Output the transformed data
columns_on_join = ['id']
columns_to_rename = {'id': 'client_identifier',
                     'btc_a': 'bitcoin_address',
                     'cc_t': 'credit_card_type'}
output_data(spark, output_path1, output_path2, config_model1, config_model2, columns_on_join,
            columns_to_rename, storage_location, output_format, mode)
logger.info("Transformed data written to storage location successfully.")



