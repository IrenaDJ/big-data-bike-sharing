from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
#import ts.flint as flint

def aggregate_data(weather, bikes, joined_path, spark = None):
    #TODO: Create data flint aggregation to work on cluster
    # if (spark == None):
    #     joined = spark.read.parquet("hdfs://localhost:9000/Data/D.C/New/joined.parquet")
    # else:
    #     flintContext = flint.FlintContext(spark)
    # return joined
    pass

def filter_data(data, start_date, end_date, allowed_stations):
    """ Filters weather data by start and end date. """

    dates = (start_date,  end_date)
    date_from, date_to = [F.to_timestamp(F.lit(s), "MM/dd/yyyy") for s in dates]
    data_filtered = data.where((data.date_time > date_from) & (data.date_time < date_to))
    data_filtered = data_filtered.where((data_filtered.end_station_id.isin(allowed_stations)) & (data_filtered.start_station_id.isin(allowed_stations)))
    return data_filtered


def data_summary(data):
    """ Summary shows classes percentage in dataset and min, max, mean and std of the attributes. """

    class_summary = data.groupBy('weather_class').count()
    class_summary.show()
    
