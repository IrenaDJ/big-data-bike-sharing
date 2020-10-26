#here we will have a model that gets trained by the hourly data
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorIndexer, VectorAssembler, StringIndexerModel, OneHotEncoderModel
from pyspark.ml.evaluation import RegressionEvaluator

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def main(stations_indexer_path, onehot_path, weather_indexer_path, model_data_folder, station_id, hour,
        month, dayofyear, visibility, air_temp, wind_speed, weather_class):

    #Starting session
    spark = SparkSession.builder.appName('BigDataML').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Loading data
    columns = ["hour", 'dayofyear', 'month', 'air_temp', 'wind_speed', 'visibility', 'weather_class', 'station']
    vals = [(hour, dayofyear, month, air_temp, wind_speed, visibility, weather_class, station_id)]
    data = spark.createDataFrame(vals, columns)

    model = PipelineModel.load(model_data_folder)
    
    stringIndexer = StringIndexerModel.load(stations_indexer_path)
    data = stringIndexer.transform(data)
    stringIndexer_weather = StringIndexerModel.load(weather_indexer_path)
    data = stringIndexer_weather.transform(data)

    encoder = OneHotEncoderModel.load(onehot_path)
    data = encoder.transform(data)

    features =\
        VectorAssembler(inputCols=["hour", 'dayofyear', 'month', 'air_temp', 'wind_speed', 'visibility', 'weather_index', 'station_vector'],
         outputCol="features")
    test_data = features.transform(data)

    model = PipelineModel.load(model_data_folder)
    # Make predictions.
    predictions = model.transform(test_data).collect()

    print("Predicted count is: {}".format(int(predictions[0]['prediction'])))



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('stations_indexer_path', type=str, 
                        help='path for tations indexer')
    parser.add_argument('station_onehot_path', type=str, 
                        help='path for weather class one hot')
    parser.add_argument('weather_indexer_path', type=str, 
                        help='path for weather class indexer')
    parser.add_argument('model_data_folder', type=str, 
                        help='a local folder for saving trained model')
    parser.add_argument('station_id', type=str, 
                        help='station id')
    parser.add_argument('--hour', type=int, default=8,
                        help='station id')
    parser.add_argument('--month', type=int, default=4,
                        help='station id')
    parser.add_argument('--dayofyear', type=int, default=110,
                        help='station id')
    parser.add_argument('--visibility', type=float, default=10.0,
                        help='station id')
    parser.add_argument('--air_temp', type=float, default=14,
                        help='station id')
    parser.add_argument('--wind_speed', type=float, default=10,
                        help='station id')
    parser.add_argument('--weather_class', type=str, default='clear',
                        help='station id')

    args = parser.parse_args()
    main(args.stations_indexer_path, args.station_onehot_path, args.weather_indexer_path, args.model_data_folder, args.station_id, args.hour, \
        args.month, args.dayofyear, args.visibility, args.air_temp, args.wind_speed, args.weather_class)