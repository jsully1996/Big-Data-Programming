from pyspark import SparkConf, SparkContext

import sys
import json

def getline(line):
    fileline = json.loads(line)
    return fileline

def addtup(t1, t2):
    return tuple(sum(x) for x in zip(t1, t2))

def get_sum_count(score_sum):
    a = score_sum[0]
    b = score_sum[1][0]
    c = score_sum[1][1]
    return a,float(b/c)

def get_avg_score(avg_sc):
    a = avg_sc[0]
    b = avg_sc[1][0]['score']
    c = avg_sc[1][1]
    d = avg_sc[1][0]['author']
    return a,b/c,d

def main():
    comments = text.map(getline).cache()  #Using cache() because json.loads is expensive
    commentbysub = comments.map(lambda c: (c['subreddit'], c)) #Form a pair RDD with the subreddit as keys and comment data as values
    c_score = comments.map(lambda c: (c['subreddit'], (c['score'],1)))
    c_scoresum = c_score.reduceByKey(addtup)
    c_avg = c_scoresum.map(get_sum_count)
    c_avgscore = c_avg.filter(lambda k_v: k_v[1]>0)
    reddits = commentbysub.join(c_avgscore)
    output_data = reddits.map(get_avg_score).sortBy(lambda t: t[1], False)
    output_data.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName("relative score")
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    text = sc.textFile(inputs)
    main()

