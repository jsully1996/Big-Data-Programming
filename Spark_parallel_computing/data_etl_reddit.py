from pyspark import SparkConf, SparkContext

import sys
import json

def getline(line):
    fileline = json.loads(line)
    sub= fileline.get("subreddit")          
    score = fileline.get("score") 
    author= fileline.get("author")            
    fields = {"subreddit": sub, "score": score, "author": author} #construct a json with only required fields
    return fields

def filterline(line):
    return 'e' in line["subreddit"]

def main(text, output):
    refline = text.map(getline)
    filtered_line = refline.filter(filterline).cache()
    positive_line = filtered_line.filter(lambda line: line["score"]>0).map(json.dumps).saveAsTextFile(output + '/positive') 
    negative_line = filtered_line.filter(lambda line: line["score"]<=0).map(json.dumps).saveAsTextFile(output + '/negative') 
    
if __name__ == '__main__':
    conf = SparkConf().setAppName("reddit etl")
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    text = sc.textFile(inputs)
    main(text, output)



