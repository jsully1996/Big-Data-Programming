import sys

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('temperature range sql').getOrCreate()

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

def get_range(values):
    values.registerTempTable('Values')

    dfrange = spark.sql("""
    SELECT v1.Date, v1.Station, (v1.Temperature-v2.Temperature) AS Range FROM
    (SELECT Station, Date, Observation, Temperature FROM Values
     WHERE Observation='TMAX') v1
     JOIN
     (SELECT Station, Date, Observation, Temperature FROM Values
     WHERE Observation='TMIN') v2
     ON (v1.Station = v2.Station AND v1.Date = v2.Date)
    """)
    dfrange.registerTempTable('RangeTable')

    df_maxrange = spark.sql("""
    SELECT Date, MAX(Range) AS MaxRange FROM RangeTable
    GROUP BY Date
    """)
    df_maxrange.registerTempTable('MaxRange')

    df_result = spark.sql("""
    SELECT t1.Date as Date, t1.Station as Station, t2.MaxRange as MaxRange FROM
    RangeTable t1
    JOIN MaxRange t2
    ON (t1.Date = t2.Date AND t1.Range = t2.MaxRange)
    """)
    return df_result

def gettextvalue(dframe):
    return str(dframe.Date)+' '+str(dframe.Station)+' '+str(dframe.MaxRange)

def main(inputs,output):
    
    df = spark.read.csv(inputs, schema = temp_schema)   
    df = df.filter(df.QFlag.isNull()).cache()

    dfrange = get_range(df)
    dfrange.show(100)
    result = dfrange.rdd.map(gettextvalue)
    outdata = result.sortBy(lambda r: r[0]).coalesce(1)
    outdata.saveAsTextFile(output)

if __name__ == "__main__":
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)

