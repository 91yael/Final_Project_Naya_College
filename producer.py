import time
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import os

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Weather_producer") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9001") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read data from MinIO
data_df = spark.read.parquet("s3a://final-project-naya-college/Parquet/")

# Convert DataFrame to JSON
data = data_df.toJSON()
print(data.take(6))

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka_broker:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

# Send data to Kafka
i = 0
for json_data in data.collect():
    try:
        i += 1
        producer.send(topic='Weather_forcast_producer', value=json_data)
        if i == 50:
            producer.flush()
            time.sleep(5)
            i = 0
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")

producer.close()
spark.stop()
