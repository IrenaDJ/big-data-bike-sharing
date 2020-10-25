#here we will have a model that gets trained by the hourly data
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main(test_folder, model_path):

    #Starting session
    spark = SparkSession.builder.appName('BigDataML').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Loading data
    test_data = spark.read.parquet(test_folder)
    test_data = test_data.withColumn('weather_class', test_data['weather_class'].cast(DoubleType()))

    features =\
        VectorAssembler(inputCols=["hour", 'dayofyear', 'month', 'air_temp', 'wind_speed', 'visibility', 'weather_class'], outputCol="features")
    test_data = features.transform(test_data)

    model = PipelineModel.load(model_path)
    # Make predictions.
    predictions = model.transform(test_data)

    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2")
    rmse = evaluator.evaluate(predictions)
    print("R2 on test data = %g" % rmse)

    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("RMSE on test data = %g" % rmse)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('test_folder', type=str, 
                        help='a hdfs folder containing training data')
    parser.add_argument('model_data_folder', type=str, 
                        help='a local folder for saving trained model')

    args = parser.parse_args()
    main(args.test_folder, args.model_data_folder)