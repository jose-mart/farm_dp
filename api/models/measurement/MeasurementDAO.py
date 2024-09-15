from pyspark.sql import SparkSession, DataFrame
from utils.spark_utils import get_config

class MeasurementDAO:
    table_name: str = "measurements/"

    def __init__(self, spark: SparkSession, env: str = "dev") -> None:
        self.spark = spark
        self.config = get_config(env)

    def read_data(self) -> DataFrame:
        data = (
            self.spark.read.format("delta")
            .load(self.config["bronze_path"] + self.table_name)
        )        
        return data
    
    def write_data(self, df: DataFrame) -> None:
        path = self.config["bronze_path"] + self.table_name
        try:
            df.write.format("delta").mode("append").save(path)
        except Exception as e:
            print("overwriting data")
            if 'path does not exist' in str(e).lower():
                df.write.format("delta").mode("overwrite").save(path)