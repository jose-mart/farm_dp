from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import sys


def get_spark_session() -> SparkSession:
    """
        Define and configure spark session
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    
    builder = SparkSession.builder \
        .appName("DeltaLakeBackend") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def get_config(env: str = "dev") -> dict:
    """
        Define config for environment. To be expanded
    """
    config = {}
    if env == "dev":
        config["bronze_path"] = "../data/farm_development/bronze/"
        config["silver_path"] = "../data/farm_development/silver/"
        config["gold_path"]   = "../data/farm_development/gold/"
    
    return config
