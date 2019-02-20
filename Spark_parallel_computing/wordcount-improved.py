import sys
import operator
import re      #import regular expression package
import string

from pyspark import SparkConf, SparkContext

inputs = sys.argv[1]
output = sys.argv[2]
wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation)) #define a word as anything separated by spaces and without punctuation

conf = SparkConf().setAppName("wordcount improved")
sc = SparkContext(conf=conf)

text = sc.textFile(inputs)


def getline(line):
    wordslist = wordsep.split(line) #split the line into tuples and store them in wordslist
    wordslist = filter(None, wordslist) #remove null words that are being considered due to re
    return wordslist

def get_key(kv): 
    return kv[0]

words = text.flatMap(lambda line: getline(line)).map(lambda w: (w.lower(), 1)) #lambda function to convert all keys to lowercase

wordcount = words.reduceByKey(operator.add)

outdata = wordcount.sortBy(get_key).map(lambda (w, c): u"%s %i" % (w, c)) #lambda function to set the output format
outdata.saveAsTextFile(output)
