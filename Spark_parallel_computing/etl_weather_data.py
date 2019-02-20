import sys

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather ETL').getOrCreate()

observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])

def main(inputs, output):
    weather = spark.read.csv(inputs, schema=observation_schema)
    weather.show() #show original data
    weather = weather.filter((weather['qflag'].isNull() & (weather['station'].startswith('CA')) & (weather['observation']=='TMAX')))
    etl_data = weather.select(weather['station'],weather['date'],(weather['value']/10).alias('tmax'))
    etl_data.show() #show data after ETL operations
    etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__=='__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)

