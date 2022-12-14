# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 kafka_data_publisher.py

import json
import findspark
findspark.init()
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("kafka_data_publisher")\
    .getOrCreate()
    
# Read the Schema of the Kafka Topic to be streamed
with open("schema.json") as f:
    df_schema = StructType.fromJson(json.load(f))
    print(df_schema.simpleString())

df = spark\
    .read\
    .option("header", "true")\
    .option("sep", ";")\
    .schema(df_schema)\
    .csv('SKAB*.csv')\
    .withColumn('value', to_json(struct(col("*"))))\
    .select("value")

df\
.write\
.format("kafka")\
.option("kafka.bootstrap.servers", 'localhost:9092')\
.option("topic", 'SKAB')\
.save()

spark.streams.awaitAnyTermination()
# spark.stop()