import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
cluster_seeds = ['199.60.17.188', '199.60.17.216']
conf = SparkConf().setAppName('load logs spark') \
        .set('spark.cassandra.connection.host', ','.join(cluster_seeds))
spark = SparkSession.builder.appName('Spark Cassandra example') \
         .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).config('spark.dynamicAllocation.maxExecutors', 16).getOrCreate()

def output_line(arg):
    orderkey, price, names = arg
    namestr = ', '.join(sorted(list(names)))
    return 'Order #%d $%.2f: %s' % (orderkey, price, namestr)

def main(user_id,output,orderkeys):
    #Create a spark Dataframe object by reading the Cassandra table
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='orders', keyspace=user_id).load()
    #Keep only values which match the orderkeys given by the user
    orders_df = orders_df.filter(orders_df['orderkey'].isin(orderkeys))   
    
    line_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='lineitem', keyspace=user_id).load()
    line_df = line_df.filter(line_df['orderkey'].isin(orderkeys))
    
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table='part', keyspace=user_id).load()
    #Conditions for joining tables
    condition1 = ['orderkey']
    tpch_df1 = orders_df.join(line_df, condition1, 'inner')
    condition2 = ['partkey']
    tpch_df2 = tpch_df1.join(part_df, condition2, 'inner')
    tpch_df2.show()
    
    #The joined table with ambiguous columns filtered out
    super_table = tpch_df2.filter(tpch_df2['orderkey'].isin(orderkeys))
    final_table = super_table.groupBy(super_table['orderkey'],super_table['totalprice']).agg(functions.collect_set(super_table['name']))
    final_table.explain()
    out = final_table.rdd.sortBy(lambda x: x[0]).map(output_line).coalesce(1)
    out.saveAsTextFile(output)
     

if __name__ == '__main__':
    user_id = sys.argv[1]
    output = sys.argv[2]
    orderkeys = list(map(int,sys.argv[3:])) #Convert orderkeys to integers before passing to main
    main(user_id,output,orderkeys)
    

