from models.cow.CowDAO import CowDAO
from models.cow.CowDTO import CowDTO
from models.cow.CowModel import CowModel

from models.measurement.MeasurementModel import MeasurementModel
from models.measurement.MeasurementDAO import MeasurementDAO

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DateType
import pandas as pd
import tempfile


class CowService:
    """
        Service class for managing cow-related operations using Apache Spark and Delta Lake.
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.cow_dao = CowDAO(spark)
        self.measurement_dao = MeasurementDAO(spark)

    def get_latest_sensor_data(self, cow_id: str) -> MeasurementModel:
        """
            Retrieves the latest sensor data for a given cow.
        """
        df = self.measurement_dao.read_data()
        filtered_df = df.filter(f"cow_id = '{cow_id}'")
        latest_record = (
            filtered_df
            .withColumn("date", from_unixtime(col("timestamp")))
            .orderBy(col("date").desc())
            .drop("date","ingestion_date")
        )

        if latest_record.count() > 0:
            return MeasurementModel(**latest_record.first().asDict())
        else:
            return f"No records found for cow_id {cow_id}"

    def add_new_cow(self, id:str, cow: CowDTO) -> CowModel:
        """
            Adds a new cow record to the Bronze Delta Lake table.
        """
        data = self.cow_dao.read_data()
        if data.filter(f"id = '{id}'").count() > 0:
            return "Cow already exists"
        else:
            schema = StructType([
                StructField("id", StringType(), nullable=False),
                StructField("name", StringType(), nullable=True),
                StructField("birthdate", DateType(), nullable=False)
            ])

            new_cow = CowModel(id, cow.name, cow.birthdate)

            pdf = pd.DataFrame([new_cow.__dict__])
            # Create a temporary directory
            with tempfile.TemporaryDirectory() as temp_dir:
                # Define the CSV file path in the temporary directory
                csv_file_path = f"{temp_dir}/cows.csv"
                
                # Write the DataFrame to CSV as workaround to a configuration issue
                pdf.to_csv(csv_file_path, index=False)

                # Convert the Pandas DataFrame to a Spark DataFrame
                sdf = self.spark.read.format("csv").load(csv_file_path, header=True, schema = schema)
                self.cow_dao.write_data(sdf)

            return new_cow