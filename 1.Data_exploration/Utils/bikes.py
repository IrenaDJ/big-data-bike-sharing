from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt

#Haversine distance
def get_distance(longit_a, latit_a, longit_b, latit_b):
    # Transform to radians
    longit_a, latit_a, longit_b, latit_b = map(radians, [longit_a,  latit_a, longit_b, latit_b])
    dist_longit = longit_b - longit_a
    dist_latit = latit_b - latit_a
    # Calculate area
    area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371
    # Calculate Distance
    distance = central_angle * radius * 1000
    return abs(round(distance))


def process_bikes(bikes_raw):
    """ Parses raw bikes data and selects only neccessary columns. """
    bike_data = bikes_raw
    bike_data = bike_data.withColumn('duration', bike_data['Duration'].cast(IntegerType()))
    bike_data = bike_data.withColumn('start_date', F.to_timestamp(bike_data['Start date'], "yyyy-MM-dd HH:mm:ss"))
    bike_data = bike_data.withColumn('end_date', F.to_timestamp(bike_data['End date'], "yyyy-MM-dd HH:mm:ss"))
    bike_data = bike_data.withColumn('start_station_id', bike_data['Start station number'])
    bike_data = bike_data.withColumn('start_station_name', bike_data['Start station'])
    bike_data = bike_data.withColumn('end_station_id', bike_data['End station number'])
    bike_data = bike_data.withColumn('end_station_name', bike_data['End station'])
    bike_data = bike_data.withColumn('bike_number', bike_data['Bike number'])
    bike_data = bike_data.withColumn('member_type', bike_data['Member type'])

    bike_data = bike_data.select('duration', 'start_date', 'end_date', 'start_station_id', 'start_station_name', 'end_station_id', 'end_station_name', 'bike_number', 'member_type')

    bike_data = bike_data.withColumn('start_station_id', F.when(bike_data['start_station_id'].like('00000'), '31127').otherwise(bike_data['start_station_id']))
    bike_data = bike_data.withColumn('end_station_id', F.when(bike_data['end_station_id'].like('00000'), '31127').otherwise(bike_data['end_station_id']))
    bike_data = bike_data.withColumn('start_station_id', F.when(bike_data['start_station_id'].isin('31008', '32031'), None).otherwise(bike_data['start_station_id']))
    bike_data = bike_data.withColumn('end_station_id', F.when(bike_data['end_station_id'].isin('31008', '32031'), None).otherwise(bike_data['end_station_id']))
    bike_data = bike_data.dropna(subset=['start_station_id', 'end_station_id'])
    #some weird date problem for 31.03.2019...
    bike_data = bike_data.dropna()
    
    return bike_data

def process_stations(stations_raw, spark):
    """ Parses raw stations data and selects only used stations and neccessary columns. """
    stations = stations_raw.select('id', 'name', 'terminalName', 'lat', 'long', 'nbBikes', 'nbEmptyDocks')

    stations = stations.withColumn('lat', stations.lat.cast(DoubleType()))
    stations = stations.withColumn('long', stations.long.cast(DoubleType()))
    stations = stations.withColumn('nbBikes', stations.nbBikes.cast(IntegerType()))
    stations = stations.withColumn('nbEmptyDocks', stations.nbEmptyDocks.cast(IntegerType()))

    station_columns = ['id', 'name', 'terminalName', 'lat', 'long', 'nbBikes', 'nbEmptyDocks']
    new_station = spark.createDataFrame([(597, 'Mount Vernon Ave & E Del Ray Ave', '31086', 38.826213, -77.058640, 5, 10)], station_columns)
    stations = stations.union(new_station)

    return stations


def filter_stations(stations, min_lat, min_long, max_lat, max_long):
    data_filtered = stations.where((stations.lat > min_lat) & (stations.lat < max_lat) & (stations.long > min_long) & (stations.long < max_long))
    stations_list = data_filtered.select('terminalName').toPandas()
    stations_set = set(stations_list['terminalName'])
    return stations_set


def filter_bikes(bikes, start_date, end_date, allowed_stations):
    dates = (start_date,  end_date)
    date_from, date_to = [F.to_timestamp(F.lit(s), "MM/dd/yyyy") for s in dates]
    bikes_filtered = bikes.where((bikes.start_date > date_from) & (bikes.start_date < date_to))
    bikes_filtered = bikes_filtered.where((bikes_filtered.end_station_id.isin(allowed_stations)) & (bikes_filtered.start_station_id.isin(allowed_stations)))
    return bikes_filtered


def bikes_summary(bikes, stations):
    member_summary = bikes.groupBy('member_type').count().sort(F.desc("count"))
    member_summary.show()
    bikes_summary = bikes.groupBy('bike_number').count().sort(F.desc("count"))
    bikes_summary.show()
    duration_summary = bikes.select([F.min("duration"), F.max("duration"), F.mean("duration"), F.stddev("duration")])
    duration_summary.show()

    #print statistics for stations check-ins and check-outs
    check_in_summary = bikes.groupBy('start_station_id').count().sort(F.desc("count"))
    check_in_summary.show()
    check_out_summary = bikes.groupBy('end_station_id').count().sort(F.desc("count"))
    check_out_summary.show()

    #print statistics for distance
    udf_get_distance = F.udf(get_distance)
    stations_partial = stations.select('terminalName', 'lat', 'long')

    bikes_joined = bikes.join(stations_partial.withColumnRenamed('lat','lat_start').withColumnRenamed('long','long_start'), bikes.start_station_id == stations.terminalName)
    bikes_joined = bikes_joined.drop('terminalName')
    bikes_joined = bikes_joined.join(stations_partial.withColumnRenamed('lat','lat_end').withColumnRenamed('long','long_end'), bikes_joined.end_station_id == stations.terminalName)
    bikes_joined = bikes_joined.drop('terminalName')

    bikes = bikes_joined.withColumn("abs_distance", udf_get_distance(
        bikes_joined.long_start, bikes_joined.lat_start,
        bikes_joined.long_end, bikes_joined.lat_end))

    distance_summary = bikes.select([F.min("abs_distance"), F.max("abs_distance"), F.mean("abs_distance"), F.stddev("abs_distance")])
    distance_summary.show()
