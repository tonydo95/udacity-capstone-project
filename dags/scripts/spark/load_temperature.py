from pyspark.sql import SparkSession
import os
import logging
import pyspark.sql.functions as F

OUTPUT_PATH = 's3a://output-data-project'
INPUT_PATH = 's3a://input-data-project'

temperature_columns = [
    'year',
    'month',
    'AverageTemperature',
    'State',
    'state_code'
]

def createTemperatureData(spark):
    temperature_df = spark.read.csv(
        INPUT_PATH + '/dimension-data/GlobalLandTemperaturesByState.csv',
        inferSchema=True,
        header=True
    )
    
    temperature_df = temperature_df.filter( (temperature_df.dt.contains('2012')) & 
                                           (F.col('Country') == 'United States'))

    temperature_df = temperature_df.select(F.year('dt').alias('year'), 
                                            F.month('dt').alias('month'), 
                                           'State', 'AverageTemperature')
    
    return temperature_df

def main():
    spark = SparkSession\
        .builder\
        .appName("Immigration_ETL")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    temperature_df = createTemperatureData(spark)
    
    us_states_df = spark.read.parquet(OUTPUT_PATH + '/dimension_tables/us_states.parquet')
    
    temperature_df = temperature_df.join(
        us_states_df,
        temperature_df.State == us_states_df.state,
        how='left'
    ).drop(us_states_df.state).select(temperature_columns)
    
    temperature_df.write.mode('overwrite').partitionBy('State', 'year').parquet(
        OUTPUT_PATH + '/dimension_tables/temperature_us_states.parquet'
    )

if __name__ == "__main__":
    main()