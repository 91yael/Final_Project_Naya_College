from kafka import KafkaConsumer
import json

def process_weather_data(data):
    # Example processing function
    print("Processing weather data for city:", data['city_name'])
    print("Data:", data['data'])

if __name__ == "__main__":
    kafka_servers = ['localhost:9092']  # Replace with your Kafka server addresses
    kafka_topic = 'weather_data'  # Topic name

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_servers,
        auto_offset_reset='earliest',  # Start reading at the earliest available message
        enable_auto_commit=True,
        group_id='weather_data_group',  # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening to Kafka topic: {kafka_topic}")

    try:
        for message in consumer:
            weather_data = message.value
            process_weather_data(weather_data)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
