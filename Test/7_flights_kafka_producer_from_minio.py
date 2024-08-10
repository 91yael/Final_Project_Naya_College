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
processed_file = 'processed_records.txt'  # File to track processed records

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

def load_processed_records():
    if os.path.exists(processed_file):
        with open(processed_file, 'r') as f:
            return set(line.strip() for line in f)
    return set()

def save_processed_record(record_id):
    with open(processed_file, 'a') as f:
        f.write(record_id + '\n')

def upload_data_from_minio_to_kafka():
    try:
        # Load already processed records
        processed_records = load_processed_records()
        
        # List all objects in the MinIO bucket under the specified folder
        objects = minio_client.list_objects(minio_bucket_name, prefix=minio_folder_name, recursive=True)
        
        for obj in objects:
            # Download object from MinIO
            response = minio_client.get_object(minio_bucket_name, obj.object_name)
            data = response.read()
            
            # Convert data to JSON
            record = json.loads(data.decode('utf-8'))
            
            # Check if the record contains the "Full data:itineraries" key
            if "Full data:itineraries" not in record:
                record_id = obj.object_name  # or another unique identifier from the record
                
                if record_id not in processed_records:
                    # Send data to Kafka topic
                    producer.send(kafka_topic, json.dumps(record).encode('utf-8'))
                    print(f"Sent data from {obj.object_name}")
                    
                    # Mark this record as processed
                    save_processed_record(record_id)
                    processed_records.add(record_id)
    
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
