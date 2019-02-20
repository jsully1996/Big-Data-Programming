import sys
from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName('read stream').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

input_topic = sys.argv[1]
msgs = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092') \
    .option('subscribe', input_topic).load()

msgs.registerTempTable("kafka_table")
kafka_df = spark.sql("""SELECT CAST(value as string) as index_col from kafka_table""")
splitcol = functions.split(kafka_df.index_col, ' ')

kafka_df = kafka_df.withColumn('x', splitcol.getItem(0))
kafka_df = kafka_df.withColumn('y', splitcol.getItem(1))
kafka_df = kafka_df.withColumn('xy',kafka_df.x*kafka_df.y)
kafka_df = kafka_df.withColumn('x2',kafka_df.x*kafka_df.x)
kafka_df.createOrReplaceTempView("new_kafka_table")

new_df = spark.sql("""Select SUM(x) as sumx, SUM(y) as sumy, SUM(xy) as sumxy, SUM(x2) as sumx2, COUNT(*) as countx from new_kafka_table""")
new_df.createOrReplaceTempView("newer_kafka_table")
slope_df = spark.sql("""SELECT (sumxy-(1/countx*(sumx*sumy)))/(sumx2 -(1/countx*(sumx*sumx))) as slope, sumx, sumy , countx from newer_kafka_table""")
slope_df.createOrReplaceTempView("slope_table")
intercept_df = spark.sql("""SELECT slope, (sumy-slope*sumx)/countx as intercept from slope_table""")

stream = intercept_df.writeStream.format('console').outputMode('update').start()
stream.awaitTermination(600)

