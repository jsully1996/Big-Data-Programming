import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temperature range').getOrCreate()

temp_schema = types.StructType([
    types.StructField('Station', types.StringType(), False),
    types.StructField('Date', types.StringType(), False),
    types.StructField('Observation', types.StringType(), False),
    types.StructField('Temperature', types.DoubleType(), False),
    types.StructField('MFlag', types.StringType(), True),
    types.StructField('QFlag', types.StringType(), True),
    types.StructField('SFlag', types.StringType(), True),
    types.StructField('OBSTime', types.StringType(), True),
    ])

def getdfmax(df):
    dfmax = df.select(df['Station'], df['Date'], df['Observation'], df['Temperature']).where(df['Observation'] == 'TMAX')
    dfmax = dfmax.withColumnRenamed('Observation', 'MaxObservation')
    dfmax = dfmax.withColumnRenamed('Temperature', 'MaxTemperature')
    return dfmax

def getdfmin(df):
    dfmin = df.select(df['Station'], df['Date'], df['Observation'], df['Temperature']).where(df['Observation'] == 'TMIN')
    dfmin = dfmin.withColumnRenamed('Observation', 'MinObservation')
    dfmin = dfmin.withColumnRenamed('Temperature', 'MinTemperature')
    return dfmin

def gettextvalue(dframe):
    return str(dframe.Date)+' '+str(dframe.Station)+' '+str(dframe.MaxRange)

def main(inputs,output):
    df = spark.read.csv(inputs, schema=temp_schema)
    df = df.filter(df.QFlag.isNull())
    #df.show()
    dfmax = getdfmax(df)
    dfmin = getdfmin(df)
    condition1 = [dfmax['Station'] == dfmin['Station'], dfmax['Date'] == dfmin['Date']]
    dfrange = dfmax.join(dfmin, condition1, 'inner').select(dfmax['Station'], dfmax['Date'], (dfmax['MaxTemperature']-dfmin['MinTemperature']).alias('Range'))
    #dfrange.show(10)
    df_maxrange = dfrange.groupby('Date').agg({"Range": "max"})
    df_maxrange = df_maxrange.withColumnRenamed('max(Range)', 'MaxRange')
    condition2 = [dfrange['Date'] == df_maxrange['Date'], dfrange['Range'] == df_maxrange['MaxRange']]
    df_result = dfrange.join(df_maxrange, condition2, 'inner').select(dfrange['Date'], dfrange['Station'], df_maxrange['MaxRange'])
    df_result.show(300)

    results = df_result.rdd.map(gettextvalue)
    outdata = results.sortBy(lambda d: d[0]).coalesce(1)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)




