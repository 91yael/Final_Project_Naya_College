# 

import os
import json
import io
import pandas as pd
from kafka import KafkaProducer
from minio import Minio
from dotenv import load_dotenv
from datetime import datetime

def get_latest_file_from_minio(minio_client, bucket_name, folder_path):
    try:
        objects = minio_client.list_objects(bucket_name, prefix=folder_path, recursive=True)
        latest_file = None
        latest_time = None
        
        for obj in objects:
            if not obj.is_dir:
                obj_time = obj.last_modified
                if latest_time is None or obj_time > latest_time:
                    latest_time = obj_time
                    latest_file = obj.object_name

        if latest_file is None:
            raise ValueError("No files found in the specified folder.")
        
        return latest_file
    except Exception as err:
        print(f"An error occurred while listing objects from MinIO: {err}")
        return None

def get_data_from_minio(minio_client, bucket_name, file_path):
    try:
        response = minio_client.get_object(bucket_name, file_path)
        data = response.read()
        df = pd.read_parquet(io.BytesIO(data))
        return df.to_dict(orient='records')
    except Exception as err:
        print(f"An error occurred while fetching data from MinIO: {err}")
        return []

def send_to_kafka(producer, topic, data):
    for record in data:
        producer.send(topic, json.dumps(record).encode('utf-8'))

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')
    minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
    minio_folder_path = os.getenv('MINIO_FOLDER_PATH_WEATHER')
    
    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER_2')

    if not minio_endpoint or not minio_access_key or not minio_secret_key or not minio_bucket_name or not minio_folder_path:
        raise ValueError("MinIO configuration not found. Make sure it's set in the .env file.")
    
    if not kafka_broker or not kafka_topic:
        raise ValueError("Kafka broker or topic not found. Make sure they are set in the .env file.")
    
    # Use secure=False for HTTP connection
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to False if MinIO is running on HTTP
    )
    
    latest_file_path = get_latest_file_from_minio(minio_client, minio_bucket_name, minio_folder_path)
    if latest_file_path:
        data = get_data_from_minio(minio_client, minio_bucket_name, latest_file_path)
        if data:
            producer = KafkaProducer(bootstrap_servers=[kafka_broker])
            send_to_kafka(producer, kafka_topic, data)

if __name__ == "__main__":
    main()
