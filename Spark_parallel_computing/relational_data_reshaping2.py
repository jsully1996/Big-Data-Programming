import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, functions, types
cluster_seeds = ['199.60.17.188', '199.60.17.216']
conf = SparkConf().setAppName('load logs spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
spark = SparkSession.builder.appName('Spark Cassandra example') \
         .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def main(in_user_id,out_user_id):
    #Create a spark Dataframe object by reading the Cassandra table
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=in_user_id).load()
    orders_df = orders_df.withColumnRenamed('comment', 'order_comment')
    line_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=in_user_id).load()
    line_df = line_df.withColumnRenamed('comment', 'lineitem_comment')
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=in_user_id).load()
    part_df = part_df.withColumnRenamed('comment', 'parts_comment')
    #Conditions for joining tables
    condition1 = ['orderkey']
    tpch_df1 = orders_df.join(line_df, condition1, 'inner')
    condition2 = ['partkey']
    tpch_df2 = tpch_df1.join(part_df, condition2, 'inner')
    tpch_df2.show()
 
    final_table = tpch_df2.groupBy('orderkey', 'totalprice', 'custkey', 'orderstatus', 'orderdate',
                                       'order_priority', 'clerk', 'ship_priority', 'order_comment').\
    agg(functions.collect_set('name').alias('part_names'))  
    final_table.show()
    final_table.write.format("org.apache.spark.sql.cassandra").options(table='orders_parts', keyspace=out_user_id).save()
    
if __name__ == '__main__':
    in_user_id = sys.argv[1]
    out_user_id = sys.argv[2]
    main(in_user_id,out_user_id)
  
