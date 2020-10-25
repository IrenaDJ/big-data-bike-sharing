import sys
from glob import glob
from kafka import KafkaProducer
import pandas as pd

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:2.4.7,org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 pyspark-shell'

from kafka.admin import KafkaAdminClient, NewTopic

def main(input_path, bootstrap_server, topic_name):

    kafka_producer = KafkaProducer(bootstrap_servers=bootstrap_server,
                                   value_serializer=lambda x: x.encode("utf-8"), api_version=(0, 1, 0))

    file = pd.read_parquet(input_path, engine='pyarrow')

    for _, el in file.iterrows():
        print(el)
        kafka_producer.send(topic_name, value = el.to_json())
        kafka_producer.flush()

    # with open(input_path, 'r') as opened_file:
    #     for line in opened_file:

    #         info = line.strip()
    #         split_info = info.split(',')
    #         n = len(split_info)
    #         if n == 0:
    #             continue
    #         if "timestamp" not in split_info[n-1].lower():
    #             kafka_producer.send(topic_name, value=info)
    #             kafka_producer.flush()
    #             print(info)

def alternative_main(input_path, bootstrap_server, topic_name):

    admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:7092", 
    client_id='test'
    )

    topic_list = []
    topic_list.append(NewTopic(name="test", num_partitions=1, replication_factor=1))
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

    #Starting session
    spark = SparkSession.builder.appName('BigData1').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    joined_raw = spark.read.parquet(input_path)

    #print(joined_raw.show())
    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    query  = joined_raw.select(F.col('time').cast("string").alias("key"), F.to_json(F.struct("duration", "visibility")).alias("value"))
    print(query.show())

    query = joined_raw \
    .select(F.col('time').cast("string").alias("key"), F.to_json(F.struct("duration", "visibility")).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_server) \
    .option("topic", "test") \
    .save()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('joined_data_folder', type=str, 
                        help='a hdfs folder containing joined data')
    parser.add_argument('bootstrap_server', type=str, 
                        help='name of the kafka broker')
    parser.add_argument('topic_name', type=str, 
                        help='kafka topic name')

    args = parser.parse_args()
    main(args.joined_data_folder, args.bootstrap_server, args.topic_name)

