import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3' # make sure we have Spark 2.3+

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    #define the indexer, assembler and classifier
    word_indexer = StringIndexer(inputCol = "word", outputCol = "labelCol", handleInvalid = 'error')
    rgb_assembler = VectorAssembler(inputCols = ['R', 'G', 'B'], outputCol = "features")
    lab_assembler = VectorAssembler(inputCols = ['labL', 'labA', 'labB'], outputCol = "features") 
    classifier = MultilayerPerceptronClassifier(maxIter = 400, layers = [3, 30, 11], blockSize = 1, seed = 123, labelCol = "labelCol")
    sqlTrans = SQLTransformer(statement = rgb_to_lab_query)
    
    # TODO: create a pipeline to predict RGB colours -> word
    
    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    
    lab_pipeline = Pipeline(stages=[sqlTrans, lab_assembler, word_indexer, classifier])
    lab_model = lab_pipeline.fit(train)
    # TODO: create an evaluator and score the validation data
    evaluator = MulticlassClassificationEvaluator(labelCol = "labelCol" , predictionCol = "prediction")
    
    #Generate plots using the function provided in colour_tools
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    plot_predictions(lab_model, 'LAB', labelCol='word')

    rgb_predictions = rgb_model.transform(validation)
    lab_predictions = lab_model.transform(validation)

    rgb_score = evaluator.evaluate(rgb_predictions)
    lab_score = evaluator.evaluate(lab_predictions)
   
    print('Validation score for RGB model: %g' % (rgb_score, ))
    print('Validation score for LAB model: %g' % (lab_score, ))
        
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
