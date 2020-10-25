from __future__ import print_function

import sys
import argparse
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#from cassandra.cluster import Cluster
from uuid import uuid1

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.7,org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 pyspark-shell'

def predict(spark, input_data, model_path):

    if input_data.isEmpty():
        print("No input data.")
        print("")
        return

    fixed_data = input_data.map(lambda x: json.loads(x[1]))
    fixed_data.cache()

    # collection = fixed_data.collect()
    # for x in collection:
    #     print(x)

    count = fixed_data.count()
    print("Total number of elements: " + str(count) + ".")

    if fixed_data.isEmpty():
        print("There are no elements so no further analysis can be made.")
        print("")
        return

    data = fixed_data
    df = data.toDF()
    for column in df.columns:
        df = df.withColumn(column, df[column].cast(DoubleType()))

    features =\
        VectorAssembler(inputCols=["hour", 'dayofyear', 'month', 'air_temp', 'wind_speed', 'visibility', 'weather_class'], outputCol="features")
    test_data = features.transform(df)

    model = PipelineModel.load(model_path)
    # Make predictions.
    predictions = model.transform(test_data)

    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("RMSE on test data = %g" % rmse)

    print("")


def main(bootstrap_server, topic_name, model_path, time_interval):

    #Starting session instead of context!!!
    spark = SparkSession.builder.appName('BigDataML').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    context = spark.sparkContext

    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {
        "metadata.broker.list": bootstrap_server})

    stream.foreachRDD(lambda input_data: predict(spark,
        input_data, model_path))

    # Zapocinje se obrada stream-a
    streaming_context.start()
    streaming_context.awaitTermination()



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data kafka consumer.')

    parser.add_argument('bootstrap_server', type=str, 
                        help='name of the kafka broker')
    parser.add_argument('topic_name', type=str, 
                        help='kafka topic name')
    parser.add_argument('model_path', type=str, 
                        help='model path')
    parser.add_argument('time_interval', type=float, 
                        help='time interval for batch processing')

    args = parser.parse_args()
    main(args.bootstrap_server, args.topic_name, args.model_path, args.time_interval)

