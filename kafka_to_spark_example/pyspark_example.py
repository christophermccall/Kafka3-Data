from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json


# 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark_example.py'

spark = SparkSession.builder \
    .appName("kafka_to_spark_example") \
    .getOrCreate()


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "listen-activity") \
    .load()


# create your schema based on the keys in the json
schema = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastname", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True)
])


# use spark to create a new DataFrame from the kafka message





query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
query.awaitTermination()

