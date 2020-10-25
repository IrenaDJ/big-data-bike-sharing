import sys
from glob import glob
from kafka import KafkaProducer
import pandas as pd
import json
import time

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

    file = pd.read_parquet(input_path)

    for _, el in file.iterrows():
        print(el)
        time.sleep(5)
        kafka_producer.send(topic_name, value = el.to_json())
        kafka_producer.flush(timeout = 5)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Big data exploration.')
    parser.add_argument('data_folder', type=str, 
                        help='a hdfs folder containing joined data')
    parser.add_argument('bootstrap_server', type=str, 
                        help='name of the kafka broker')
    parser.add_argument('topic_name', type=str, 
                        help='kafka topic name')

    args = parser.parse_args()
    main(args.data_folder, args.bootstrap_server, args.topic_name)

