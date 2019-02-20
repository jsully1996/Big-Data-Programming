import sys
import ntpath

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()

pages_schema = types.StructType([
    types.StructField('language', types.StringType(), False),
    types.StructField('page_name', types.StringType(), False),
    types.StructField('count', types.LongType(), False),
    types.StructField('size', types.LongType(), False),
    types.StructField('filename', types.StringType(), False),
])

def getfilename(filepath):
    part1, part2 = ntpath.split(filepath)   #method to get file name
    return part2[11:22]  #get substring from file name


def makeint(time):
    time = int(time[0:8]+time[9:11])
    return time

def main(inputs, outputs):
    pages = spark.read.csv(inputs, schema = pages_schema, sep=' ')
    #extract filename from input_file_name() 
    filename_to_time = functions.udf(getfilename, returnType = types.StringType())
    pages = pages.withColumn('filename', filename_to_time(functions.input_file_name()))
    pages.show()
    #apply all filters
    filtered_pages = pages.filter((pages['language'] == 'en') & (pages['page_name'] != 'Main_Page') & (pages['page_name'].startswith('Special:') == False))
    filtered_pages.cache()
    #find max viewed pages
    grouped_pages = filtered_pages.groupBy('filename')
    max = grouped_pages.agg(functions.max('count').alias('max_views'))
    #To allow tie
    joined_pages = filtered_pages.join(max, on=((filtered_pages['count'] == max['max_views']) & (filtered_pages['filename'] == max['filename'])))
    #filename to int type to sort
    inttime = functions.udf(makeint, returnType = types.FloatType())
    new_pages = joined_pages.withColumn('date', inttime(filtered_pages['filename']))

    output = new_pages.select(filtered_pages['filename'],'page_name','max_views')
    output.explain()
    output.sort('date').write.csv(outputs, mode='overwrite')
    output.sort('date').show()

if __name__=='__main__':
    inputs = sys.argv[1]
    outputs = sys.argv[2]
    main(inputs, outputs)
