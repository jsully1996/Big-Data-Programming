import sys
import math
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

cluster_seeds = ['199.60.17.171', '199.60.17.188']
conf = SparkConf().setAppName('Correlate Logs').set('spark.cassandra.connection.host', ','.join(cluster_seeds))
spark = SparkSession.builder.appName('Spark Cassandra example') \
         .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()

def get_tuple(a, b):
    return tuple(sum(x) for x in zip(a, b))

def get_req_data(line):
    hst = line['host']
    count = 1 
    byt = int(line['bytes'])
    return hst, (count, byt)

def apply_filter(x):
    if x is not None:
        return x

def get_sums(request_log):
    xn, yn = request_log[1]
    return 1, xn, yn, xn ** 2, yn ** 2, xn * yn

def get_corr(n,x,y,x2,y2,xy):
    a = n*xy
    b = x*y
    num = a-b
    de1 = math.sqrt((n * x2) - (x ** 2))
    de2 = math.sqrt((n * y2) - (y ** 2)) 
    denom = de1*de2
    res = num/denom
    return res

def main(user_id, table_name):
    #Read the data from cassandra table into a spark dataframe object
    logs_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table_name, keyspace=user_id).load()
    logs_df.show()
    
    #Convert spark dataframe to RDD
    logs_rdd = logs_df.rdd

    #Get RDD containing key: hostname 
    log_req_data = logs_rdd.map(get_req_data)

    #Filter values that dont satisfy re
    log_req_data = log_req_data.filter(apply_filter)

    #Calculate sums for each host
    log_sums_host = log_req_data.reduceByKey(get_tuple)

    # Calculate individual values of each host
    log_sums = log_sums_host.map(get_sums)

    # Calculate total sums
    n, x, y, x_2, y_2, xy = log_sums.reduce(get_tuple)

    # Calculate r 
    r = get_corr(n,x,y,x_2,y_2,xy)
    print("r = " + str(r))
    print("r ^ 2 = " + str(r ** 2))

if __name__ == "__main__":
    user_id = sys.argv[1]
    table_name = sys.argv[2]
    main(user_id, table_name)
 



