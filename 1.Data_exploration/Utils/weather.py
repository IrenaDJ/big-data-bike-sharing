from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


def process_weather(weather_raw):
    """ Parses raw weather data and selects only neccessary columns. """

    weather = weather_raw.dropna(subset=['Station_ID', 'Date_Time'])
    weather = weather.dropna(subset=['air_temp_set_1', 'wind_speed_set_1'])

    weather_short = weather.withColumn('Date_Time', weather.Date_Time.substr(0, 16))

    weather_cast = weather_short.withColumn('date_time', F.to_timestamp(weather_short.Date_Time, "MM/dd/yyyy HH:mm"))
    weather_cast = weather_cast.withColumn('air_temp', weather_cast.air_temp_set_1.cast(DoubleType()))
    weather_cast = weather_cast.withColumn('wind_speed', weather_cast.wind_speed_set_1.cast(DoubleType()))
    weather_cast = weather_cast.withColumn('visibility', weather_cast.visibility_set_1.cast(DoubleType()))
    weather_cast = weather_cast.withColumn('weather_summary', weather_cast.weather_summary_set_1d)
    weather_cast = weather_cast.withColumn('weather_condition', weather_cast.weather_condition_set_1d)

    weather = weather_cast
    weather = weather.select('date_time', 'air_temp', 'wind_speed', 'weather_summary', 'weather_condition', 'visibility')
    #From Farenheit to Celsius
    weather = weather.withColumn('air_temp', (weather.air_temp - 32) * 5.0/9.0)

    weather = weather.withColumn('weather_class', F.when(weather.weather_summary.rlike('(clear|scattered|sunny|obscured)'), 'clear').otherwise(weather.weather_summary))
    weather = weather.withColumn('weather_class', F.when(weather.weather_summary.rlike('(broken|overcast|cloudy)'), 'cloudy').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_summary.rlike('(light (drizzle|rain))|rain|drizzle'), 'light rain').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_summary.rlike('(heavy (drizzle|rain))|heavy_rain'), 'heavy rain').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_class.rlike('snow'), 'snowy').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_class.rlike('ice'), 'icy').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_class.rlike('thunder|squalls'), 'stormy').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_class.rlike('haze|mist|fog'), 'foggy').otherwise(weather.weather_class))
    weather = weather.withColumn('weather_class', F.when(weather.weather_summary.isNull(), 'clear').otherwise(weather.weather_class))

    return weather

def filter_weather(weather, start_date, end_date):
    """ Filters weather data by start and end date. """

    dates = (start_date,  end_date)
    date_from, date_to = [F.to_timestamp(F.lit(s), "MM/dd/yyyy") for s in dates]
    weather_filtered = weather.where((weather.date_time > date_from) & (weather.date_time < date_to))
    return weather_filtered


def weather_summary(weather):
    """ Summary shows classes percentage in dataset and min, max, mean and std of the attributes. """

    class_summary = weather.groupBy('weather_class').count()
    class_summary.show()
    temp_summary = weather.select([F.min("air_temp"), F.max("air_temp"), F.mean("air_temp"), F.stddev("air_temp")])
    temp_summary.show()
    wind_summary = weather.select([F.min("wind_speed"), F.max("wind_speed"), F.mean("wind_speed"), F.stddev("wind_speed")])
    wind_summary.show()
    visibility_summary = weather.select([F.min("visibility"), F.max("visibility"), F.mean("visibility"), F.stddev("visibility")])
    visibility_summary.show()
    