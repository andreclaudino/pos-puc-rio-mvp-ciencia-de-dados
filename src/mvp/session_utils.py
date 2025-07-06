from typing import Optional
from pyspark.sql import SparkSession
import os
import sys

def create_spark_session() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark
