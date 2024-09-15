from datetime import date
from models.cow import CowModel, CowDTO
from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import get_config
from utils.config import environment


class CowDAO:
    table_name: str = "cows"

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = get_config(environment.env)

    def read_data(self) -> DataFrame:
        path = self.config["silver_path"] + self.table_name
        data = (
            self.spark.read.format("delta")
            .load(path)
        )
   
        return data
    
    def write_data(self, df: DataFrame) -> None:
        path = self.config["bronze_path"] + self.table_name
        df.write.format("delta").mode("append").save(path)