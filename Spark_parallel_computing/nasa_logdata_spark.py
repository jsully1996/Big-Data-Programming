import uuid
import sys
import re
import datetime
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types, Row
cluster_seeds = ['199.60.17.188', '199.60.17.216']
conf = SparkConf().setAppName('load logs spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
spark = SparkSession.builder.appName('Spark Cassandra example') \
         .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()


log_dissemble = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')

def get_values(line):
    values = log_dissemble.split(line)
    if len(values) == 6:
          host = values[1]
          path = int(values[4])
          dtime = datetime.datetime.strptime(values[2], '%d/%b/%Y:%H:%M:%S')
          num_bytes = values[3]
          id_value= str(uuid.uuid4())
          return host, id_value, path, dtime, num_bytes

def apply_filter(x):
    if x is not None:
        return x

def main(inputs, user_id, table_name):
    log_lines = spark.sparkContext.textFile(inputs)
    log_lines = log_lines.map(get_values)
    filt_log_lines = log_lines.filter(apply_filter)
    part_log_lines = filt_log_lines.repartition(50)
    
    logs = spark.createDataFrame(part_log_lines).toDF("host", "id", "bytes", "datetime", "path")
    logs.write.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=user_id).save()

if __name__ == "__main__":
    inputs = sys.argv[1]
    user_id = sys.argv[2]
    table_name = sys.argv[3]
    main(inputs, user_id, table_name)
 

 

