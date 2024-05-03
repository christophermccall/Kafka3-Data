from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, from_json

# this line of code is how we deploy are spark app
# 'spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark_example.py'

# a lot like flask. this is where are app is created when we run the program
spark = SparkSession.builder \
    .appName("kafka_to_spark_example") \
    .getOrCreate()

# this is the spark equivalent of a consumer
# you can also look for patterns in all topics! .option("subscribePattern", "topic.*") \
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "listen-activity") \
    .load()
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# create your schema based on the keys in the json
# much like how we create schema in sqlalchemy
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


# parse the json and use the schema

parsed_df = df.withColumn("data", from_json(col("value"), schema))

# try counting the number of times an artist/song appears in the listen-events

song_plays = parsed_df.select(
    col("data.song").alias("song")
).groupBy("song").count()

artist_listens = parsed_df.select(
    col("data.artist").alias("artist")
).groupBy("artist").count()


# how many users in each city?

users_in_city = parsed_df.select(
    col("data.city").alias("city")
).groupBy("city").count()

# use spark to create a new DataFrame from the kafka message
# set the log level to avoid getting too many info-level logs every for every execution.
spark.sparkContext.setLogLevel("WARN")
query = song_plays \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("complete") \
    .start()
query.awaitTermination()

