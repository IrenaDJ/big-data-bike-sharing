import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from Utils.weather import *
from Utils.bikes import *
from Utils.aggregation import *

def main(weather_data_folder, stations_data_folder, bikes_data_folder, joined_data_folder, start_date, end_date, 
    min_lat, min_long, max_lat, max_long):

    #Starting session
    spark = SparkSession.builder.appName('BigData1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #Loading data
    weather_raw = spark.read.option("header", True).csv(weather_data_folder, sep=',', comment = '#')
    stations_raw = spark.read.option("header", True).csv(stations_data_folder, sep=',', comment = '#')
    bikes_raw = spark.read.option("header", True).csv(bikes_data_folder, sep=',', comment = '#')

    weather = process_weather(weather_raw)
    weather = filter_weather(weather, start_date, end_date)
    #weather_summary(weather)

    stations = process_stations(stations_raw, spark)
    allowed_stations = filter_stations(stations, min_lat, min_long, max_lat, max_long)

    bikes = process_bikes(bikes_raw)
    bikes = filter_bikes(bikes, start_date, end_date, allowed_stations)
    bikes = merge_bikes_and_stations(bikes, stations)
    bikes_summary(bikes, stations)

    #Getting aggregated data - for starters

    #TODO: make conditional reading or generating the correct file
    joined_raw = spark.read.parquet(joined_data_folder)
    joined = filter_data(joined_raw, start_date, end_date, allowed_stations)
    data_summary(joined)




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('weather_data_folder', type=str, 
                        help='a hdfs folder containing weather data')
    parser.add_argument('stations_data_folder', type=str, 
                        help='a hdfs folder containing stations')
    parser.add_argument('bikes_data_folder', type=str, 
                        help='a hdfs folder containing bikes info')
    parser.add_argument('joined_data_folder', type=str, 
                        help='a hdfs folder containing joined data')

    parser.add_argument('--startdate', default="12/31/2018",
                        help='start date for data MM/dd/yyyy')
    parser.add_argument('--enddate', default="12/31/2019",
                        help='end date for data MM/dd/yyyy')
    parser.add_argument('--minlat', default="38",
                        help='minimum latitude')
    parser.add_argument('--minlong', default="-78",
                        help='minimum longitude')
    parser.add_argument('--maxlat', default="40",
                        help='maximum latitude')
    parser.add_argument('--maxlong', default="-76",
                        help='maximum longitude')

    args = parser.parse_args()
    main(args.weather_data_folder, args.stations_data_folder, args.bikes_data_folder, args.joined_data_folder, 
    args.startdate, args.enddate, 
    args.minlat, args.minlong, args.maxlat, args.maxlong)