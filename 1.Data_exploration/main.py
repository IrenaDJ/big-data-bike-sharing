import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from Utils.weather import *
from Utils.bikes import *

def main(weather_data_folder, stations_data_folder, bikes_data_folder, start_date, end_date):

    #Starting session
    spark = SparkSession.builder.appName('BigData1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Loading data
    weather_raw = spark.read.option("header", True).csv(weather_data_folder, sep=',', comment = '#')
    stations_raw = spark.read.option("header", True).csv(stations_data_folder, sep=',', comment = '#')
    bikes_raw = spark.read.option("header", True).csv(bikes_data_folder, sep=',', comment = '#')

    weather = process_weather(weather_raw)
    weather = filter_weather(weather, start_date, end_date)
    weather_summary(weather)

    stations = process_stations(stations_raw, spark)
    bikes = process_bikes(bikes_raw)
    bikes = filter_bikes(bikes, start_date, end_date)
    bikes_summary(bikes, stations)



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('weather_data_folder', type=str, 
                        help='a hdfs folder containing weather data')
    parser.add_argument('stations_data_folder', type=str, 
                        help='a hdfs folder containing stations')
    parser.add_argument('bikes_data_folder', type=str, 
                        help='a hdfs folder containing bikes info')
    parser.add_argument('--startdate', default="12/31/2018",
                        help='start date for data MM/dd/yyyy')
    parser.add_argument('--enddate', default="12/31/2019",
                        help='end date for data MM/dd/yyyy')

    args = parser.parse_args()
    main(args.weather_data_folder, args.stations_data_folder, args.bikes_data_folder, args.startdate, args.enddate)