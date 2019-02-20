from pyspark import SparkConf, SparkContext

import sys
import json

def getline(line):
    fileline = json.loads(line)
    return fileline

def add_tup(t1, t2):
    return tuple(sum(x) for x in zip(t1, t2))

def get_relscore(bcast, key, comments):
    avg_score = bcast.value.get(key)
    score = comments['score']
    author = comments['author']
    relative_score = score/avg_score
    return key, relative_score, author

def get_sum_count(score_sum):
    a = score_sum[0]
    b = score_sum[1][0]
    c = score_sum[1][1]
    return a,float(b/c)

def main():
    comments = text.map(getline).cache()
    commentbysub = comments.map(lambda c: (c['subreddit'], c))
    c_score = comments.map(lambda c: (c['subreddit'], (c['score'], 1)))
    c_scoresum = c_score.reduceByKey(add_tup)
    c_avg = c_scoresum.map(get_sum_count) 
    c_avgscore = c_avg.filter(lambda k_v: k_v[1]>0)
    avg_dict = dict(c_avgscore.collect())
    broad_avg_dict = sc.broadcast(avg_dict)
    output_data = commentbysub.map(lambda kcomm : get_relscore(broad_avg_dict, kcomm[0], kcomm[1])).sortBy(lambda t: t[1], False)
    output_data.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score broadcast')
    sc = SparkContext(conf=conf)
    inputs = sys.argv[1]
    output = sys.argv[2]
    text = sc.textFile(inputs)
    main()



