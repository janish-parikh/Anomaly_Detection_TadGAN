# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 inference.py

import pickle
import json
import findspark
findspark.init()
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from orion.primitives.tadgan import score_anomalies
from utils import time_segments_aggregate, rolling_window_sequences,compute_intervals, evaluate

THRESHOLD = 2 # Error Threshold
CHANNEL = 0 # index of channel to reconstruct
RE_ERROR_TYPE = 'dtw' #Reconstruction Error Type
TIMECOLUMN = 'timestamp' #Timestamp index in the data
KAFKA_TOPIC = 'SKAB' #kafka topic
MODEL_PATH = 'models/tadgan.pkl' #path to model
TARGET_COL = 0 #Column to reconstruct
APP_NAME = 'anomaly_detection' #Spark App name

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName(APP_NAME)\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

streaming_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers",'localhost:9092')\
        .option("failOnDataLoss",False)\
        .option("subscribe", KAFKA_TOPIC)\
        .option("startingOffsets", "earliest")\
        .option("minOffsetsPerTrigger", 100)\
        .load()

# Read the Schema of the Kafka Topic to be streamed
with open("schema.json") as f:
    df_schema = StructType.fromJson(json.load(f))
    print(df_schema.simpleString())

streaming_df = streaming_df.selectExpr("CAST(value AS STRING)")\
    .select(from_json(col("value"), df_schema))\
    .select("from_json(value).*")
streaming_df = streaming_df.toDF(*(c.replace('.', '_') for c in streaming_df.columns))

model = pickle.load(open(MODEL_PATH, 'rb'))

def run_inference(data, epoch_id):
    data = data.toPandas()
    X, index = time_segments_aggregate(data, interval=1, time_column=TIMECOLUMN)
    print("Batch input shape: {}".format(X.shape))
    print("Training data index shape: {}".format(index.shape))
    X, _, X_index, _ = rolling_window_sequences(X, index, window_size=100, target_size=1, step_size=1,target_column=TARGET_COL)
    y = X[:, :, CHANNEL:CHANNEL+1]
    X_hat, critic = model.predict(X,y)
    error, _, _, _ = score_anomalies(X[:,:,[0]], X_hat, X_index, critic, rec_error_type= RE_ERROR_TYPE, comb="mult")
    anomalous_data = compute_intervals(error, index,THRESHOLD)
    anomalous_data.to_parquet(path ="results/" + str(epoch_id))

query1 = (streaming_df\
.writeStream\
.format("console")\
.outputMode("append")\
.foreachBatch(run_inference)\
.start())

spark.streams.awaitAnyTermination()