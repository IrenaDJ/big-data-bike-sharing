#here we will have a model that gets trained by the hourly data
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main(train_data_folder, model_path):

    #Starting session
    spark = SparkSession.builder.appName('BigDataML').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Loading data
    data = spark.read.parquet(train_data_folder)
    data = data.withColumn('weather_class', data['weather_class'].cast(DoubleType()))

    features =\
        VectorAssembler(inputCols=["hour", 'dayofyear', 'month', 'air_temp', 'wind_speed', 'visibility', 'weather_class'], outputCol="features")
    train_data = features.transform(data)

    # Train a GBT model.
    gbt = GBTRegressor(featuresCol="features", maxIter=10)

    # Chain indexer and GBT in a Pipeline
    pipeline = Pipeline(stages=[gbt])

    # Train model.  This also runs the indexer.
    model = pipeline.fit(train_data)
    model.write().overwrite().save(model_path)

    # Make predictions.
    predictions = model.transform(train_data)

    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="r2")
    rmse = evaluator.evaluate(predictions)
    print("R2 on train data = %g" % rmse)

    gbtModel = model.stages[0]
    print(gbtModel)  # summary only


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('train_folder', type=str, 
                        help='a hdfs folder containing training data')
    parser.add_argument('model_data_folder', type=str, 
                        help='a local folder for saving trained model')

    args = parser.parse_args()
    main(args.train_folder, args.model_data_folder)