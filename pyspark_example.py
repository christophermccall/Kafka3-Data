from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("kafka_to_spark_example") \
    .getOrCreate()

# use spark to create a new DataFrame from the kafka message

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "listen-events") \
    .option("includeHeaders", "true") \
    .load()

query = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("") \
    .start()
query.awaitTermination()
