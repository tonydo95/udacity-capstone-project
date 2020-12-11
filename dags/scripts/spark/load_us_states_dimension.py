from pyspark.sql import SparkSession
import configparser
import os
import logging
import pyspark.sql.functions as F

OUTPUT_PATH = 's3a://output-data-project'
INPUT_PATH = 's3a://input-data-project'


def createUSStatesDf(spark):
    
    us_cities_df = spark.read.csv(
        INPUT_PATH + '/dimension-data/us-cities-demographics.csv',
        sep=";",
        inferSchema=True,
        header=True
    )
    
    us_cities_df = us_cities_df.selectExpr(
        "State as state",
        "`Male Population` as male_population",
        "`Female Population` as female_population",
        "`Total Population` as total_population",
        "`Foreign-born` as foreign_born",
        "`State Code` as state_code",
        "Race as race",
        "Count as count"
    ).fillna({'foreign_born' : 0, 'male_population':0, 'female_population':0, 'total_population':0})
    
    us_states_df = us_cities_df.filter(F.col('race') == 'White').groupBy("state", "state_code") \
                    .agg(
                         F.sum("male_population").alias("total_male_population"), \
                         F.sum("female_population").alias("total_female_population"), \
                         F.sum("total_population").alias("total_population"), \
                         F.sum("foreign_born").alias("total_foreign_born") \
                        ).orderBy(F.col('state')).dropDuplicates()
    
    return us_states_df

def main():
    
    spark = SparkSession\
        .builder\
        .appName("Immigration_ETL")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    us_states_df = createUSStatesDf(spark)
    
    us_states_df.write.mode('overwrite').parquet(
        OUTPUT_PATH + '/dimension_tables/us_states.parquet')
    
    
if __name__ == "__main__":
    main()