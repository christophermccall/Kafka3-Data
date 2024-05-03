from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.streaming import StreamingQuery
import os


# os.system('spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark_example.py')

spark = SparkSession.builder \
    .appName("kafka_to_spark_example") \
    .getOrCreate()

# use spark to create a new DataFrame from the kafka message

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "listen-activity") \
    .load()

query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()
query.awaitTermination()
