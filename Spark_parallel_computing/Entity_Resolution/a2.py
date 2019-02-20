import sys
import re
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
spark = SparkSession.builder.appName('entity resolution').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

'''
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.functions import udf

conf = SparkConf().setAppName("entity resolution")
sc = SparkContext(conf=conf)
assert sc.version >= '1.5.1'
sqlCt = SQLContext(sc)
'''

def get_words(r, stopwords):
    words = re.split('\W+', r)
    words = [i.lower() for i in words if i not in stopwords and i!= '']
    return words


def token_distribute(r):
    id = r[0]
    token_str = r[1]
    m = re.search('\[(.*?)\]', token_str)
    tokens = m.group(1).split(', ')
    token_id_map = [(t, id) for t in tokens]

    return token_id_map


def jaccard_similarity(r):
    m = re.search('\[(.*?)\],\[(.*?)\]', r)
    if m is None: return 0
    jk1 = set((m.group(1).split(', ')))
    jk2 = set((m.group(2).split(', ')))
    if len(jk1) == 0 or len(jk2) == 0: return 0

    common_tokens = [t for t in jk2 if t in jk1]
    combined_len = len(jk1) + len(jk2) - len(common_tokens)
    if combined_len == 0:
        return 0
    return float(len(common_tokens))/combined_len



class EntityResolution:
    def __init__(self, dataFile1, dataFile2, stopWordsFile):
        self.f = open(stopWordsFile, "r")
        self.stopWords = set(self.f.read().split("\n"))
        self.stopWordsBC = spark.sparkContext.broadcast(self.stopWords).value
        self.df1 = spark.read.parquet(dataFile1).cache()
        self.df2 = spark.read.parquet(dataFile2).cache()


    def preprocessDF(self, df, cols):
        stopwords = self.stopWordsBC
        transform_udf = functions.udf(lambda r: get_words(r, stopwords))
        preprocessed_df = df.withColumn("joinKey", transform_udf(functions.concat_ws(' ', df[cols[0]], df[cols[1]])))

        return preprocessed_df


    def filtering(self, df1, df2):
        flat_rdd1 = df1.select(df1.id, df1.joinKey).rdd.map(token_distribute).flatMap(lambda t: t)
        flat_rdd2 = df2.select(df2.id, df2.joinKey).rdd.map(token_distribute).flatMap(lambda t: t)

        flat_df1 = spark.createDataFrame(flat_rdd1, ('token1', 'id1'))
        flat_df2 = spark.createDataFrame(flat_rdd2, ('token2', 'id2'))

        cond = [flat_df1.token1 == flat_df2.token2]
        joined_df = flat_df2.join(flat_df1, cond).select(flat_df1.id1, flat_df2.id2).dropDuplicates()

        cond1 = [df1.id == joined_df.id1]
        new_df1 = joined_df.join(df1, cond1).select(joined_df.id1, df1.joinKey.alias('joinKey1'), joined_df.id2)

        cond2 = [df2.id == new_df1.id2]
        new_df2 = new_df1.join(df2, cond2)\
            .select(new_df1.id1, new_df1.joinKey1, new_df1.id2, df2.joinKey.alias('joinKey2'))
        new_df2.show()

        return new_df2


    def verification(self, candDF, threshold):
        jaccard_udf = functions.udf(lambda r: jaccard_similarity(r))
        jaccard_df = candDF.withColumn("jaccard", jaccard_udf(functions.concat_ws(',', candDF.joinKey1, candDF.joinKey2)))
        return jaccard_df.where(jaccard_df.jaccard >= threshold)


    def evaluate(self, result, groundTruth):
        countR = len(result)
        lstT = [t for t in result if t in groundTruth]
        countT = float(len(lstT))
        precision = countT/countR

        countA = len(groundTruth)
        recall = countT/countA

        fmeasure = 2*precision*recall/(precision+recall)

        return (precision, recall, fmeasure)


    def jaccardJoin(self, cols1, cols2, threshold):
        newDF1 = self.preprocessDF(self.df1, cols1)
        #newDF1.show(n=1,truncate=False)
        newDF2 = self.preprocessDF(self.df2, cols2)
        #newDF2.show(n=1,truncate=False)
        print("Before filtering: %d pairs in total" %(self.df1.count()*self.df2.count()))

        candDF = self.filtering(newDF1, newDF2)
        print("After Filtering: %d pairs left" %(candDF.count()))

        resultDF = self.verification(candDF, threshold)
        resultDF.show(truncate=False)
        print("After Verification: %d similar pairs" %(resultDF.count()))

        return resultDF


    def __del__(self):
        self.f.close()


if __name__ == "__main__":
    amazon_input = sys.argv[1]
    google_input = sys.argv[2]
    stopwords_input = sys.argv[3]
    perfect_mapping_input = sys.argv[4]
    er = EntityResolution(amazon_input, google_input, stopwords_input)
    amazonCols = ["title", "manufacturer"]
    googleCols = ["name", "manufacturer"]
    resultDF = er.jaccardJoin(amazonCols, googleCols, 0.5)
    resultDF.show(truncate=False)

    result = resultDF.map(lambda row: (row.id1, row.id2)).collect()
    groundTruth = spark.read.parquet(perfect_mapping_input) \
                          .map(lambda row: (row.idAmazon, row.idGoogle)).collect()
    print("(precision, recall, fmeasure) = ", er.evaluate(result, groundTruth))