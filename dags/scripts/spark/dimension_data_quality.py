from pyspark.sql import SparkSession
import logging
from glob import glob
import os

INPUT_PATH = 's3a://output-data-project'

def main():
    
    logger = logging.getLogger()
  
    dimension_paths = [
        INPUT_PATH + '/dimension_tables/temperature_us_states.parquet',
        INPUT_PATH + '/dimension_tables/us_states.parquet'
    ]

    spark = SparkSession\
        .builder\
        .appName("Immigration_ETL")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()\

    spark.sparkContext.setLogLevel("ERROR")
  
    for path in dimension_paths:
        logger.error(path)
        quality_check = spark.read.parquet(path)
        if quality_check.count() == 0 or quality_check.count() is None:
            raise Exception('Loading dimension data failed')

if __name__ == "__main__":
    main()