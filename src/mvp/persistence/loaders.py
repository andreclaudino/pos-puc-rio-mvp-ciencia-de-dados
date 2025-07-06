from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def load_data_source(spark: SparkSession, source_path: str) -> DataFrame:
    """
    Load data from a specified source path.
    
    Args:
        source_path (str): The path to the data source.
    
    Returns:
        Data loaded from the specified source.
    """
    
    data = spark\
        .read\
        .option("mergeSchema", "true")\
        .option("header", "true")\
        .option("delimiter", ";")\
        .option("quote", "\"")\
        .option("encoding", "Latin1")\
        .csv(path = source_path)
    return data


def csv_folder_to_parquet(spark: SparkSession, folder_path: str, output_path: str) -> None:
    source_frame = spark\
        .read\
        .option("headers", "true")\
        .csv(folder_path)
    
    source_frame.write.parquet(output_path, mode="overwrite")