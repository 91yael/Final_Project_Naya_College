# kafka_producer.py

import json
import requests
from kafka import KafkaProducer
import os
from dotenv import load_dotenv

def get_weather_data(api_key, cities):
    # Base URL for the Weatherbit API
    url = 'https://api.weatherbit.io/v2.0/forecast/daily'
    
    # List to store the results
    weather_data = []

    for city in cities:
        # Parameters for the API request
        params = {
            'key': api_key,
            'city': city,
            'days': 14  # Number of days for the forecast
        }
        
        # Make the GET request to the Weatherbit API
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx and 5xx)
            data = response.json()
            for forecast in data['data']:
                forecast['city'] = city
            # Add the data to the list
            weather_data.extend(data['data'])
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred for {city}: {http_err}")
        except Exception as err:
            print(f"An error occurred for {city}: {err}")

    return weather_data

def send_to_kafka(producer, topic, data):
    for record in data:
        producer.send(topic, json.dumps(record).encode('utf-8'))

def main():
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv('WEATHER_API_KEY')
    if not api_key:
        raise ValueError("API key not found. Make sure it's set in the .env file.")

    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER_1')

    if not kafka_broker or not kafka_topic:
        raise ValueError("Kafka broker or topic not found. Make sure they are set in the .env file.")

    producer = KafkaProducer(bootstrap_servers=[kafka_broker])

    # List of cities
    cities = ["Rome", "Paris", "London", "New York", "Athens", "Barcelona", "Madrid", "Praha", "Budapest"]

    weather_data = get_weather_data(api_key, cities)

    send_to_kafka(producer, kafka_topic, weather_data)

if __name__ == "__main__":
    main()
