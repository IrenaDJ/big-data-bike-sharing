from __future__ import print_function

import sys
import argparse
import json
from datetime import datetime
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from uuid import uuid1

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.7,org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 pyspark-shell'

def analysis(input_data, cassandra_session, num_stations, absdist, 
    minlat, minlong, maxlat, maxlong):

    if input_data.isEmpty():
        print("No input data.")
        print("")
        return

    fixed_data = input_data.map(lambda x: json.loads(x[1]))
    fixed_data.cache()
    # for x in fixed_data.collect():
    #     print(x)
    count = fixed_data.count()
    print("Total number of elements: " + str(count) + ".")

    #filtering
    data = fixed_data
    data = data.filter(lambda x: ((float(x["lat_start"]) >= float(minlat)) & (float(x["lat_start"]) < maxlat) & 
    (float(x["long_start"]) > minlong) & (float(x["long_start"]) < maxlong) & (float(x["abs_distance"]) > absdist)))

    if data.isEmpty():
        print("There are no elements so no further analysis can be made.")
        print("")
        return

    attributes = ["air_temp", "wind_speed", "visibility", "duration", "abs_distance"]

    for _, attribute in enumerate(attributes):
        atr_min = data.map(lambda x: float(x[attribute])).min()
        atr_max = data.map(lambda x: float(x[attribute])).max()
        atr_mean = data.map(lambda x: float(x[attribute])).mean()
        atr_std = data.map(lambda x: float(x[attribute])).stdev()

        cassandra_session.execute("""INSERT INTO bikes.Summary (attribute, timestamp, elements, min, max, mean, std) 
        VALUES (%s, toTimeStamp(now()), %s, %s, %s, %s, %s)""", (
        attribute, count, atr_min, atr_max, atr_mean, atr_std))

        print(attribute + " range: [" +
        str(atr_min) + ", " + str(atr_max) + "].")
        print(attribute + " mean: " + str(atr_mean) + ".")
        print(attribute + " standard deviation: " + str(atr_std) + ".")

    category_attributes = ["member_type", "weather_class", "start_station_id"]

    # categorical distributions check
    for _, attribute in enumerate(category_attributes):
        grouped = data.groupBy(lambda x: x[attribute])
        agg_sorted = grouped.map(lambda x: (x[0], len(x[1]))).sortBy(lambda x: x[1])
        categories = agg_sorted.map(lambda x: x[0]).collect()
        counts = agg_sorted.map(lambda x: x[1]).collect()
        if (attribute == "start_station_id") & (num_stations < len(categories)):
            categories = categories[:num_stations]
            counts = counts[:num_stations]
        print(categories)
        print(counts)

        cassandra_session.execute("""INSERT INTO bikes.Categorical_summary 
        (attribute, timestamp, elements, categories, values) VALUES (%s, toTimeStamp(now()), %s, %s, %s)""", 
        (attribute, count, categories, counts))

    print("")


def main(bootstrap_server, topic_name, num_stations, time_interval, absdist, 
    minlat, minlong, maxlat, maxlong):

    # Cassandra session
    cassandra_cluster = Cluster()
    cassandra_session = cassandra_cluster.connect(
        'bikes', wait_for_all_pools=True)

    configuration = SparkConf().setAppName("BigDataProj2")
    context = SparkContext(conf=configuration)
    context.setLogLevel("ERROR")

    streaming_context = StreamingContext(context, time_interval)
    stream = KafkaUtils.createDirectStream(streaming_context, [topic_name], {
        "metadata.broker.list": bootstrap_server})

    stream.foreachRDD(lambda input_data: analysis(
        input_data, cassandra_session, num_stations, absdist, 
    minlat, minlong, maxlat, maxlong))

    streaming_context.start()
    streaming_context.awaitTermination()


def alternative_main(bootstrap_server, topic_name, time_interval):

    #Starting session
    spark = SparkSession.builder.appName('BigData1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(topic_name)

    df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data kafka consumer.')

    parser.add_argument('bootstrap_server', type=str, 
                        help='name of the kafka broker')
    parser.add_argument('topic_name', type=str, 
                        help='kafka topic name')
    parser.add_argument('num_stations', type=int, 
                        help='top N stations')
    parser.add_argument('time_interval', type=float, 
                        help='time interval for batch processing')
    parser.add_argument('--minlat', default="38", type=float,
                        help='minimum latitude')
    parser.add_argument('--minlong', default="-78", type=float,
                        help='minimum longitude')
    parser.add_argument('--maxlat', default="40", type=float,
                        help='maximum latitude')
    parser.add_argument('--maxlong', default="-76", type=float,
                        help='maximum longitude')
    parser.add_argument('--absdist', default="-1.0", type=float,
                        help='maximum longitude')

    args = parser.parse_args()
    main(args.bootstrap_server, args.topic_name, args.num_stations, args.time_interval, args.absdist, 
    args.minlat, args.minlong, args.maxlat, args.maxlong)
    #alternative_main(args.bootstrap_server, args.topic_name, args.time_interval)

