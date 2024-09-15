import sys
import os

sys.path.append(os.path.abspath('../'))

from utils.config import environment

environment.env = "test"

import pytest
from models.cow.CowDAO import CowDAO 
from utils.spark_utils import get_spark_session, get_config
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date
import pandas as pd
import tempfile
import shutil
import os

environment.env = "test"

@pytest.fixture(scope="session", autouse=True)
def setup_session():
    set_up_datasets()

@pytest.fixture
def spark():
    with get_spark_session() as mock_spark:
        yield mock_spark

@pytest.fixture
def config():
    yield get_config("test")

def test_read_data(spark, config):
    dao = CowDAO(spark=spark)
    data = spark.read.format("delta").load("./datasets/silver/cows")

    dao_data = dao.read_data()

    assert data.count() == dao_data.count()

def test_write_data(spark: SparkSession, config):
    dao = CowDAO(spark=spark)
    result_df_count = spark.read.format("delta").load("./datasets/bronze/cows").count()
    

    # Generate extra record
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("birthdate", DateType(), False)
    ])
    data = {
        "id": ["1"],
        "name": ["Bessie"],
        "birthdate": [date(2020, 1, 1)]
    }

    pdf = pd.DataFrame([data])
    with tempfile.TemporaryDirectory() as temp_dir:
        # Define the CSV file path in the temporary directory
        csv_file_path = f"{temp_dir}/cows.csv"
        
        # Write the DataFrame to CSV as workaround to a configuration issue
        pdf.to_csv(csv_file_path, index=False)

        # Convert the Pandas DataFrame to a Spark DataFrame
        sdf = spark.read.format("csv").load(csv_file_path, header=True, schema = schema)
        dao.write_data(sdf)

        # Assert
        assert spark.read.format("delta").load(config["bronze_path"] + "cows").count() == result_df_count + 1

def set_up_datasets():
    source_folder = "./datasets/"
    target_folder = "../../data/farm_testing/"

    if os.path.exists(target_folder):
        shutil.rmtree(target_folder)

    shutil.copytree(source_folder, target_folder)

if __name__ == "__main__":
    pytest.main()
