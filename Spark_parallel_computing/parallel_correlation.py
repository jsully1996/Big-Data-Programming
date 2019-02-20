import math
import re
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Row

spark = SparkSession.builder.appName('example').getOrCreate()
sc = spark.sparkContext

log_schema = types.StructType([
    types.StructField('hostname', types.StringType(), False),
    types.StructField('num_bytes', types.IntegerType(), False),
    ])

def get_row(line):
    line_dissemble = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_dissemble, line)
    if match:
        m = re.match(line_dissemble, line)
        host = m.group(1)
        bytes = int(m.group(4))
        row = Row(host, bytes)
        return row
    return None
    
def create_row_rdd(in_directory):
    log_lines = sc.textFile(in_directory)
    log_lines = log_lines.map(get_row).filter(lambda x : x is not None)
    #print(log_lines.take(10))
    
    return log_lines 

def calculate_correlation_coefficient(row):
    n = row[0]
    sig_xi = row[1]
    sig_xi2 = row[2]
    sig_yi = row[3]
    sig_yi2 = row[4]
    sig_xiyi = row[5]
    numerator = (n*sig_xiyi)-(sig_xi*sig_yi)
    denominator_left = math.sqrt((n*sig_xi2)-(sig_xi**2))
    denominator_right = math.sqrt((n*sig_yi2)-(sig_yi**2))
    denominator = denominator_left*denominator_right
    return numerator/denominator

def main(inputs):
    
    
    logs = spark.createDataFrame(create_row_rdd(inputs),log_schema)
    
    sum_request_bytes = logs.groupby('host').agg(functions.sum('bytes').alias("sum_request_bytes")).cache()
    sum_request_bytes.show()
    count_requests = logs.groupby('host').agg(functions.count('host').alias("count_requests")).cache()
    count_requests.show()
    data = count_requests.join(sum_request_bytes, "host").cache()
    
    data = data.withColumn('1', functions.lit(1))
    
    data = data.withColumn('xi', data.count_requests)
    data.show()
    data = data.withColumn('xi^2', pow(data.count_requests, 2))  
    data = data.withColumn('yi', data.sum_request_bytes)
    data = data.withColumn('yi^2', pow(data.sum_request_bytes, 2))
    data = data.withColumn('xiyi', data.count_requests * data.sum_request_bytes)
    data = data.drop("sum_request_bytes", "count_requests")
    
    data = data.groupby().sum()
     
    data.show()
    row = data.first()
    r = calculate_correlation_coefficient(row)
    
    #print("r = %g\nr^2 = %g" % (r, r**2))
    print(r, r**2)
    print('**************************************************************************************************************************')     
    
    

if __name__ == '_main_':
    inputs = sys.argv[1]
    main(inputs)
