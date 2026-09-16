import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_DIR = "/app/raw"
PROCESSED_DIR = "/app/processed"

def get_spark():
    return (
        SparkSession.builder.appName("TickerFlowTransform")
        .master("local[*]") #local[*] means "use all available CPU cores on this machine as if they were a mini cluster."
        .getOrCreate()
    )