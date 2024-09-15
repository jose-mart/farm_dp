from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip
import os
import sys
from typing import List
from delta.tables import DeltaTable
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, current_timestamp
import functools as ft
from pyspark.sql.utils import AnalysisException


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

def basic_merge(batch_df: DataFrame, batch_id: int, unique_columns: List[str], spark:SparkSession, target_path: str, target_schema: StructType) -> None:
    """
        Basic merger for data pipelines. It creates tables if not exist or upsert data into them.
        Deduplicate records based on unique keys. If not specified it deduplicates exactly duplicated rows.
    """
    # Drop duplicates
    if unique_columns is not []:
        df_dedup = batch_df.dropDuplicates(unique_columns)
    else:
        df_dedup = batch_df.dropDuplicates()

    try:
        target = DeltaTable.forPath(spark, target_path)
    except AnalysisException as e:
        if "is not a delta table" not in str(e).lower():
            raise

        df_dedup.write.format("delta").save(target_path)
        return

    
    eqs = [col(f"l.{c}") == col(f"r.{c}") for c in unique_columns]
    conds = ft.reduce(lambda a,b: a & b, eqs)

    merge = (
        target.alias("l")
        .merge(df_dedup.alias("r"), conds)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )

    merge.execute()


def enrich_df(df: DataFrame) -> DataFrame:
    """Enrich dataframe with ingestion date."""
    return (
        df.withColumn("ingestion_date", current_timestamp())
    )