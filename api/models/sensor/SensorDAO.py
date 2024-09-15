from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import get_config
from utils.config import environment

class SensorDAO:
    table_name: str = "sensors/"

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.config = get_config(environment.env)


    def read_data(self) -> DataFrame:
        data = (
            self.spark.read.format("delta")
            .load(self.config["bronze_path"] + self.table_name)
        )        
        return data
    
    def write_data(self, df: DataFrame) -> None:
        path = self.config["bronze_path"] + self.table_name
        df.write.format("delta").mode("append").save(path)
