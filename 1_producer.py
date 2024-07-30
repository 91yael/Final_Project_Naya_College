import json
import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from minio_utils import MinioClient

# Load environment variables from .env file
load_dotenv()

def extract_city_weather_data(weather_data):
    city_weather_info = {}
    
    for city, details in weather_data.items():
        city_info = {
            "city_name": details["city_name"],
            "country_code": details["country_code"],
            "latitude": details["lat"],
            "longitude": details["lon"],
            "state_code": details["state_code"],
            "timezone": details["timezone"],
            "forecast": []
        }
        
        for day in details["data"]:
            day_info = {
                "date": day["datetime"],
                "temperature": {
                    "high_temp": day["high_temp"],
                    "low_temp": day["low_temp"],
                    "average_temp": day["temp"]
                },
                "precipitation": day["precip"],
                "humidity": day["rh"],
                "wind": {
                    "speed": day["wind_spd"],
                    "direction": day["wind_cdir"],
                    "gust_speed": day["wind_gust_spd"]
                },
                "weather": {
                    "description": day["weather"]["description"],
                    "icon": day["weather"]["icon"]
                },
                "uv_index": day["uv"],
                "visibility": day["vis"]
            }
            city_info["forecast"].append(day_info)
        
        city_weather_info[city] = city_info
    
    return city_weather_info

def send_to_kafka(topic, data, kafka_servers):
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    bucket_name = os.getenv('MINIO_BUCKET_NAME') # Replace with your bucket name
    file_path = 'Weather_Data/'  # Directory prefix for the parquet files
    kafka_servers = [os.getenv('KAFKA_BROKER')]  # Replace with your Kafka server addresses
    kafka_topic = 'weather_data'  # Replace with your Kafka topic

    city_details = {
        "Rome": {"lat": "41.89193", "lon": "12.51133", "country_code": "IT", "state_code": "07", "timezone": "Europe/Rome"},
        "Paris": {"lat": "48.85341", "lon": "2.3488", "country_code": "FR", "state_code": "11", "timezone": "Europe/Paris"},
        "London": {"lat": "51.51279", "lon": "-0.09184", "country_code": "GB", "state_code": "ENG", "timezone": "Europe/London"},
        "New York": {"lat": "40.71427", "lon": "-74.00597", "country_code": "US", "state_code": "NY", "timezone": "America/New_York"},
        "Athens": {"lat": "37.97945", "lon": "23.71622", "country_code": "GR", "state_code": "ESYE31", "timezone": "Europe/Athens"},
        "Barcelona": {"lat": "41.38506", "lon": "2.1734", "country_code": "ES", "state_code": "CT", "timezone": "Europe/Madrid"},
        "Madrid": {"lat": "40.4165", "lon": "-3.70256", "country_code": "ES", "state_code": "MD", "timezone": "Europe/Madrid"},
        "Praha": {"lat": "50.08804", "lon": "14.42076", "country_code": "CZ", "state_code": "10", "timezone": "Europe/Prague"},
        "Budapest": {"lat": "47.49801", "lon": "19.03991", "country_code": "HU", "state_code": "BU", "timezone": "Europe/Budapest"}
    }

    # Create an instance of MinioClient
    minio_client = MinioClient()

    try:
        # Get the latest parquet file
        latest_file = minio_client.get_latest_file(bucket_name, file_path)
        if latest_file:
            print(f"Processing latest file: {latest_file}")
            df = minio_client.read_parquet_file(bucket_name, latest_file)
            
            # Convert DataFrame to dictionary format expected by extract_city_weather_data
            weather_data = {}
            for city, details in city_details.items():
                city_df = df[df['city'] == city]
                if not city_df.empty:
                    city_data = {
                        "city_name": city,
                        "country_code": details["country_code"],
                        "lat": details["lat"],
                        "lon": details["lon"],
                        "state_code": details["state_code"],
                        "timezone": details["timezone"],
                        "data": city_df.to_dict(orient='records')
                    }
                    weather_data[city] = city_data
                else:
                    print(f"No data found for city: {city}")
            
            city_weather_info = extract_city_weather_data(weather_data)
            
            for city, data in city_weather_info.items():
                send_to_kafka(kafka_topic, data, kafka_servers)
    except Exception as e:
        print(f"Error: {e}")
