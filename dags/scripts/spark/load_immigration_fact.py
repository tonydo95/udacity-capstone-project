from pyspark.sql import SparkSession
from pyspark import SparkConf
from functools import reduce
from pyspark.sql import DataFrame
import boto3
import pyspark.sql.functions as F

INPUT_PATH = 's3a://input-data-project/'
OUTPUT_PATH = 's3a://output-data-project'


def createImmigrationRDD(spark, files):
    immigration_df = spark.read.json(files)
    
    immigration_df = immigration_df.filter(
        (immigration_df.i94addr.isNotNull()) & (immigration_df.i94mode == 1)
    ).select(
        immigration_df.i94yr.alias('arrival_year'),
        immigration_df.i94mon.alias('arrival_month'),
        immigration_df.i94addr.alias('state'),
        immigration_df.arrdate.alias('arrival_date'),
        immigration_df.depdate.alias('departure_date'),
        immigration_df.i94bir.alias('age'),
        immigration_df.i94visa.alias('visa_categories'),
        immigration_df.gender
    ).dropDuplicates()

    immigration_df = immigration_df.withColumn('person_id', F.monotonically_increasing_id())
    return immigration_df

def main():
    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") 
    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .enableHiveSupport()\
        .getOrCreate()
    
    sas_files = INPUT_PATH + 'immigration-data/*'
    immigration_df = createImmigrationRDD(spark, sas_files)

    immigration_df = immigration_df.withColumn('sas_date', F.lit('1960-01-01'))
    immigration_df = immigration_df.withColumn('arrival_date', F.expr("date_add(sas_date, arrival_date)")) \
                    .withColumn('departure_date', F.expr("date_add(sas_date, departure_date)")).drop('sas_date')

    immigration_df = immigration_df.withColumn('stayed_days', F.when(F.col('departure_date').isNull(), None) \
                                               .otherwise(F.expr("DATEDIFF(departure_date, arrival_date)")))

    immigration_df = immigration_df.withColumn('visa_categories', F.expr("case when visa_categories = 1.0 then 'Business' " +
                                                                        "when visa_categories = 2.0 then 'Pleasure' " +
                                                                        "else 'Student' end"))
    
    us_states_df = spark.read.parquet(OUTPUT_PATH + '/dimension_tables/us_states.parquet')

    states = us_states_df.select(F.col('state_code'))
    immigration_df = immigration_df.join(
                            states,
                            states.state_code == immigration_df.state
                    ).drop(states.state_code)

    immigration_df.write.mode('overwrite').partitionBy('state', 'arrival_month').parquet(
        OUTPUT_PATH + '/fact_tables/immigration_data.parquet')
    
    spark.stop()
    
if __name__ == "__main__":
    main()