from confluent_kafka import Consumer, KafkaException
import json
import os
from avia_scripts.mongo_utils import MongoClientWrapper
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER'),
    'group.id': 'weather_data_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['weather_data'])


mongo_client = MongoClientWrapper()

# Main Loop to Consume Messages and Insert into MongoDB
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException.PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        
        # Process the message (assuming the message is in JSON format)
        message_data = json.loads(msg.value().decode('utf-8'))
        mongo_client.insert_data_to_mongodb(message_data)

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer connection
    consumer.close()
