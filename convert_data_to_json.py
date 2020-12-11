from pyspark.sql import SparkSession
import os

def main():
    spark = SparkSession.builder\
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport()\
        .getOrCreate()

    for root, dirs, files in os.walk('./raw_ras_data'):
        for file in files:
            file_path = os.path.join(root, file)
            df_spark = spark.read.format('com.github.saurfang.sas.spark').load(file_path)
            new_path = file.split('.')[0]
            df_spark.write.json('./data/immigration-data/' + new_path)

if __name__ == "__main__":
    main()