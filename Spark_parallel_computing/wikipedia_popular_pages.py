
from pyspark import SparkConf, SparkContext

import sys
import operator
import string



inputs = sys.argv[1]
output = sys.argv[2]
conf = SparkConf().setAppName('wikipedia popular')
sc = SparkContext(conf=conf)

def get_line(line):          
     wordlist = line.split() 
     wordlist[3] = int(wordlist[3]) #Convert the view count to int
     return tuple(wordlist)         #Return a tuple of 5 elements   

def get_kvpair(tup):                #Return a key_value pair of (dateTime,(Viewcount,pageTitle))
     return (tup[0],(tup[3],tup[2])) 

def tab_separated(tup):
     return "%s\t%s" % (tup[0], tup[1])

text = sc.textFile(inputs)
new_line = text.map(get_line)
filtered_line = new_line.filter(lambda line : line[1]=='en' and not line[2]== 'Main_Page' and not line[2].startswith('Special:')) #apply all filters
#print(filtered_line.take(5))
kv_line = filtered_line.map(get_kvpair)
max_line = kv_line.reduceByKey(max)
outdata = max_line.sortBy(lambda x : x[0]).map(tab_separated) #sort by time
outdata.saveAsTextFile(output)

