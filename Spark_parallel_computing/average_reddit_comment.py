
from pyspark import SparkConf, SparkContext

import sys
import json


def getline(line):
    fileline = json.loads(line)
    key = fileline.get("subreddit")          #get key
    count = 1                                #initialize counter
    score = fileline.get("score")             
    keyval = {"key": key, "pair": (count, score)} #define value pair
    return keyval


def addkeyval(xy1, xy2):  #add key value pairs
    (x1, y1) = xy1    
    (x2, y2) = xy2
    return x1+x2, y1+y2


def main():
    refline = text.map(getline)
    lines = refline.map(lambda keyval: (keyval.get("key"), keyval.get("pair")))
    better_lines = lines.reduceByKey(addkeyval).map(lambda key_count_score: (key_count_score[0], float(key_count_score[1][1])/key_count_score[1][0])) #reduce and then map the lambda function for calculating averages
    still_better_lines = better_lines.map(lambda line: json.dumps(line))
    still_better_lines.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName("reddit averages")
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    text = sc.textFile(inputs)
    main()



