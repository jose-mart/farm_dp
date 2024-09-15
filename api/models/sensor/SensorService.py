from models.sensor.SensorModel import SensorModel
from models.sensor.SensorDAO import SensorDAO
from models.sensor.SensorDTO import SensorDTO

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, max
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
import tempfile

from utils.config import environment


class SensorService:
    """
        Service class for managing cow-related operations using Apache Spark and Delta Lake.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sensor_dao = SensorDAO(spark)


    def add_new_sensor(self, sensor_id:str, sensor: SensorDTO) -> SensorModel:
        """
            Adds a new sensor record to the Bronze Delta Lake table.
        """
        schema = StructType([
            StructField("id", StringType(), nullable=False),
            StructField("unit", StringType(), nullable=False)
        ])

        new_sensor = SensorModel(
            id=sensor_id, 
            unit=sensor.unit
        )
        

        pdf = pd.DataFrame([new_sensor.__dict__])

        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Define the CSV file path in the temporary directory
            csv_file_path = f"{temp_dir}/sensor.csv"
            
            # Write the DataFrame to CSV as workaround to a configuration issue
            pdf.to_csv(csv_file_path, index=False)

            # Convert the Pandas DataFrame to a Spark DataFrame
            sdf = self.spark.read.format("csv").load(csv_file_path, header=True, schema = schema)

            self.sensor_dao.write_data(sdf)

            return new_sensor
        