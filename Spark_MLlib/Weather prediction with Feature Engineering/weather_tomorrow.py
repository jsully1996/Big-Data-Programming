import sys
import datetime
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather tomorrow').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from pyspark.ml import PipelineModel

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType())
])


def main(model_file):
    tmax_model = PipelineModel.load(model_file)
    input_today = ('bigdata-lab',datetime.date(2018, 11, 12),49.2771, -122.9146, 330.0,12.0)
    input_yest = ('bigdata-lab',datetime.date(2018, 11, 11),49.2771, -122.9146, 330.0,11.0)
    test_df = spark.createDataFrame([input_today, input_yest],tmax_schema)
    predictions = tmax_model.transform(test_df)
    predictions.show()
    tmax_tmrw = predictions.select('prediction')
    print('Predicted tmax tomorrow :', list(*tmax_tmrw.collect())[0])

if __name__ == '__main__':
    model_file = sys.argv[1]
    main(model_file)

