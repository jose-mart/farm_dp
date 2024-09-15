from models.cow.CowDAO import CowDAO
from models.cow.CowDTO import CowDTO
from models.cow.CowModel import CowModel

from models.measurement.MeasurementModel import MeasurementModel
from models.measurement.MeasurementDAO import MeasurementDAO
from models.measurement.MeasurementDTO import MeasurementDTO

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import pandas as pd
import tempfile


class MeasurementService:
    """
        Service class for managing measurement-related operations using Apache Spark and Delta Lake.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.measurement_dao = MeasurementDAO(spark)


    def add_new_measure(self, sensor_id:str, cow_id:str, measurement: MeasurementDTO) -> MeasurementModel:
        """
            Adds a new measure record to the Bronze Delta Lake table.
        """
        schema = StructType([
            StructField("sensor_id", StringType(), nullable=False),
            StructField("cow_id", StringType(), nullable=False),
            StructField("timestamp", DoubleType(), nullable=False),
            StructField("value", DoubleType(), nullable=True)
        ])

        new_measurement = MeasurementModel(
            sensor_id=sensor_id, 
            cow_id=cow_id, 
            timestamp=measurement.timestamp, 
            value=measurement.value
        )
        

        pdf = pd.DataFrame([new_measurement.__dict__])
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Define the CSV file path in the temporary directory
            csv_file_path = f"{temp_dir}/measurement.csv"
            
            # Write the DataFrame to CSV as workaround a configuration issue
            pdf.to_csv(csv_file_path, index=False)

            # Convert the Pandas DataFrame to a Spark DataFrame
            sdf = self.spark.read.format("csv").load(csv_file_path, header=True, schema = schema)

            self.measurement_dao.write_data(sdf)

            return new_measurement