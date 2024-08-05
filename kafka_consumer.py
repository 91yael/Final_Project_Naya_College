from kafka import KafkaConsumer
from minio_utils import MinioClient
import json
import os
from dotenv import load_dotenv
import pandas as pd
import io

def save_data_to_minio(topic, data):
    folder_path = determine_folder_path(topic)
    file_name = f"{folder_path}/{topic}_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
    minio_client.upload_parquet_to_minio(bucket_name, file_name, data)
    print(f"Data saved to MinIO at {file_name}")

def determine_folder_path(topic):
    if 'weather' in topic:
        return os.getenv('MINIO_FOLDER_PATH_WEATHER')
    elif 'flights' in topic:
        return os.getenv('MINIO_FOLDER_PATH_FLIGHTS')
    else:
        return 'unknown_folder'

def consume_and_save(consumer):
    for message in consumer:
        topic = message.topic
        data = message.value
        # Print data for debugging
        print(f"Received data from topic {topic}: {data}")
        save_data_to_minio(topic, data)

def create_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[kafka_broker],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id=group_id
    )

if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    kafka_broker = os.getenv('KAFKA_BROKER')
    bucket_name = os.getenv('MINIO_BUCKET_NAME')
    
    if not kafka_broker or not bucket_name:
        raise ValueError("Kafka broker or MinIO bucket name not found. Make sure they are set in the .env file.")

    minio_client = MinioClient()

    consumers = {
        'weather-group': create_consumer(os.getenv('KAFKA_TOPIC_WEATHER_1'), 'weather-group'),
        'flights-group': create_consumer(os.getenv('KAFKA_TOPIC_FLIGHTS_3'), 'flights-group')
    }
    
    print("Starting Kafka consumers...")
    for group_id, consumer in consumers.items():
        consume_and_save(consumer)
