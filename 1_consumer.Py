from kafka import KafkaConsumer
import json

def process_weather_data(data):
    required_keys = ['city_name', 'data']
    if not all(key in data for key in required_keys):
        print(f"Missing required keys in data: {data}")
        return

    try:
        city_name = data['city_name']
        weather_info = data['data']
        
        # Example processing function
        print(f"Processing weather data for city: {city_name}")
        print("Data:", weather_info)

        # Add any city-specific processing here
        if city_name in ['Rome', 'Paris', 'London', 'New York', 'Athens', 'Barcelona', 'Madrid', 'Praha', 'Budapest']:
            # Example: Different logic for each city (if needed)
            pass  # Replace with actual processing logic

    except KeyError as e:
        print(f"Missing key in data: {e}")
    except Exception as e:
        print(f"Error processing data: {e}")

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
