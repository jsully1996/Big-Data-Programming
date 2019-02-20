
from pyspark import SparkConf, SparkContext
import sys
import math

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
import re, datetime, uuid
from pyspark.sql import SQLContext, Row, SparkSession, functions, types
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

cluster_seeds = ['199.60.17.188', '199.60.17.216']
spark = SparkSession.builder.appName('Spark Cassandra load table').config('spark.cassandra.connection.host',
                                                                          ','.join(cluster_seeds)).config(
    'spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(x):
    orderkey, price, names = x
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def read_table(key_space, table):
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=key_space).load()
    return df

def get_order_keys(order_keys):
    sql = '('
    for i in order_keys:
        sql = sql + str(i) + ','
    sql = sql[:-1] + ')'
    return sql

def main(key_space, out_dir, order_keys):
    keys = get_order_keys(order_keys)
    orders = read_table(key_space, 'orders_parts').where('orderkey in' + keys).select('orderkey','totalprice','part_names')
    output = orders.rdd.map(tuple)
    output = output.map(output_line)
    output.saveAsTextFile(out_dir)

# orderkey, price, names
if __name__ == "__main__":
    key_space = sys.argv[1]
    out_dir = sys.argv[2]
    order_keys = sys.argv[3:]
    main(key_space, out_dir, order_keys)



