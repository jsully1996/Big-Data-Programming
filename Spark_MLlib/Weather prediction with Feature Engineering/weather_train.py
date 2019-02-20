import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.DoubleType()),
])

def main(inputs, model_file):
    data = spark.read.csv(inputs, schema=tmax_schema)
    data.registerTempTable('yesterday')
    #wthr_query = """SELECT  dayofyear(date) as dayofyr, latitude, longitude, elevation,tmax  FROM __THIS__"""
    wthr_query = """SELECT dayofyear(today.date) as dayofyr,today.latitude, today.longitude, today.elevation, today.tmax, yesterday.tmax as yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station"""
    
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    #define the assembler and regressor
    assembler = VectorAssembler(inputCols=["latitude", "longitude", "elevation", "dayofyr", "yesterday_tmax"], outputCol="features")
    regressor = RandomForestRegressor(maxDepth=10, minInstancesPerNode=2, minInfoGain=0.5, labelCol = "tmax")
    trans_query = SQLTransformer(statement = wthr_query)
    
    #define pipeline and model
    wthr_pipeline = Pipeline(stages=[trans_query, assembler, regressor])
    wthr_model = wthr_pipeline.fit(train)
 
    #define the regression evaluator
    evaluator = RegressionEvaluator(labelCol="tmax", predictionCol="prediction")
    predictions = wthr_model.transform(validation)
    err = evaluator.evaluate(predictions)
    wthr_model.write().overwrite().save(model_file)
    print('Root Mean Square Error(rmse) : ' + str(err))

if __name__ == '__main__':
    inputs = sys.argv[1]
    model_file = sys.argv[2]
    main(inputs,model_file)

