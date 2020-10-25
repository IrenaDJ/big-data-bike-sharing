import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


def main(data_path, train_data_path, test_data_path, stations_path, weather_path):
    #Starting session
    #config = pyspark.SparkConf().setAll([('spark.executor.memory', '4g')])
    spark = SparkSession.builder.appName('BigData1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    data = spark.read.parquet(data_path)
    data = data.withColumn('label', data['count'].cast(DoubleType()))
    data = data.drop("count")

    stringIndexer = StringIndexer(inputCol="station", outputCol="station_index")
    data = stringIndexer.fit(data).transform(data)
    stringIndexer.write().overwrite().save(stations_path)
    stringIndexer_weather = StringIndexer(inputCol="weather_class", outputCol="weather_index")
    data = stringIndexer_weather.fit(data).transform(data)
    stringIndexer_weather.write().overwrite().save(weather_path)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])
    trainingData.write.parquet(train_data_path)
    testData.write.parquet(test_data_path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('data_path', type=str, 
                        help='a hdfs folder containing complete data')
    parser.add_argument('train_data_path', type=str, 
                        help='a path for saving train data')
    parser.add_argument('test_data_path', type=str, 
                        help='a path for saving test data')
    parser.add_argument('station_indexer_data_path', type=str, 
                        help='a path for saving train data')
    parser.add_argument('weather_indexer_data_path', type=str, 
                        help='a path for saving train data')

    args = parser.parse_args()
    main(args.data_path, args.train_data_path, args.test_data_path, args.station_indexer_data_path, args.weather_indexer_data_path)