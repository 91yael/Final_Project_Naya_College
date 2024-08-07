import os
import json
from io import BytesIO
from dotenv import load_dotenv
from kafka import KafkaConsumer
from minio import Minio
from minio.error import S3Error
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Initialize parameters
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC_FLIGHTS_3')
minio_endpoint = os.getenv('MINIO_ENDPOINT')
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
minio_folder_name = os.getenv('MINIO_FOLDER_PATH_FLIGHTS')  # Folder path in MinIO

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset='earliest',
    group_id='my-group'
)

# Initialize MinIO client
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

# Check if the bucket exists, create if not
if not minio_client.bucket_exists(minio_bucket_name):
    minio_client.make_bucket(minio_bucket_name)

def upload_to_minio(filename, data):
    try:
        # Use BytesIO to create a file-like object from the bytes data
        data_stream = BytesIO(data)
        
        minio_client.put_object(
            minio_bucket_name,
            filename,
            data_stream,
            length=len(data),
            content_type='application/json'
        )
        print(f"Uploaded {filename} to MinIO")
    except S3Error as e:
        print(f"Error uploading to MinIO: {e}")

def main():
    for message in consumer:
        record = json.loads(message.value.decode('utf-8'))

        # Create folder with date and destination countries
        from_city = record.get("from_city", "unknown")
        to_city = record.get("to_city", "unknown")
        folder_name = f"{minio_folder_name}/{datetime.now().strftime('%Y-%m-%d')}_{from_city}_{to_city}"

        # Create filename with folder path
        filename = f"{folder_name}/flight_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}_{message.offset}.json"
        
        # Convert record to JSON string and encode to bytes
        data = json.dumps(record).encode('utf-8')
        
        upload_to_minio(filename, data)

if __name__ == "__main__":
    main()

    
