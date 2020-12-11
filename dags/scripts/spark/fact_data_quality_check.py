from pyspark.sql import SparkSession
import logging
from glob import glob

INPUT_PATH = 's3a://output-data-project'

def main():
    
    spark = SparkSession\
        .builder\
        .appName("Immigration_ETL")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()\

    spark.sparkContext.setLogLevel("ERROR")
  
    
    fact_data = spark.read.parquet(INPUT_PATH + '/fact_tables/immigration_data.parquet')
    
    if fact_data.count() == 0 or fact_data.count() is None:
        raise Exception('Loading fact data failed')

if __name__ == "__main__":
    main()