import os
import json
from io import BytesIO
from dotenv import load_dotenv
from kafka import KafkaProducer
from minio import Minio
from minio.error import S3Error

# Load environment variables from .env file
load_dotenv()

# Initialize parameters
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC_FLIGHTS_4') 
minio_endpoint = os.getenv('MINIO_ENDPOINT')
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_bucket_name = os.getenv('MINIO_BUCKET_NAME')
minio_folder_name = os.getenv('MINIO_FOLDER_PATH_FLIGHTS')  


# Initialize MinIO client
minio_client = Minio(
    minio_endpoint,
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafka_broker,
    acks='all',
    retries=5,
    request_timeout_ms=30000,
    batch_size=16384,
    linger_ms=5000
)

def upload_data_from_minio_to_kafka():
    try:
        # List all objects in the MinIO bucket under the specified folder
        objects = minio_client.list_objects(minio_bucket_name, prefix=minio_folder_name, recursive=True)
        
        for obj in objects:
            # Download object from MinIO
            response = minio_client.get_object(minio_bucket_name, obj.object_name)
            data = response.read()
            
            # Convert data to JSON
            record = json.loads(data.decode('utf-8'))
            
            # Send data to Kafka topic
            producer.send(kafka_topic, json.dumps(record).encode('utf-8'))
            print(f"Sent data from {obj.object_name} to Kafka topic '{kafka_topic}'")
    
    except S3Error as e:
        print(f"Error interacting with MinIO: {e}")
    except Exception as e:
        print(f"Error processing data: {e}")
    finally:
        # Ensure all messages are sent
        producer.flush()
        producer.close()

if __name__ == "__main__":
    upload_data_from_minio_to_kafka()
