import json
from kafka import KafkaConsumer
import os
from dotenv import load_dotenv
import pandas as pd
import io
from datetime import datetime
from minio_utils import MinioClient

def upload_to_minio(data, bucket_name):
    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)

    # Convert the DataFrame to Parquet format
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = f'Weather_Data/weather_forecast_{current_date}.parquet'
    
    # Create an instance of MinioClient
    minio_client = MinioClient()

    # Upload the DataFrame as a Parquet file to MinIO
    minio_client.upload_parquet_to_minio(bucket_name, file_path, df)

def main():
    # Load environment variables from .env file
    load_dotenv()

    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER_1')
    minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')

    if not kafka_broker or not kafka_topic or not minio_bucket_name:
        raise ValueError("Kafka broker, topic or MinIO bucket name not found. Make sure they are set in the .env file.")

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_data_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    weather_data = []
    
    for message in consumer:
        weather_data.append(message.value)
        
        # Optional: you can add a condition to stop consuming after certain amount of data
        if len(weather_data) >= 100:  # Example condition
            break

    upload_to_minio(weather_data, minio_bucket_name)

if __name__ == "__main__":
    main()
