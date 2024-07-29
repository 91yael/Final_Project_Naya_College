import json
import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from minio_utils import MinioClient

# Load environment variables from .env file
load_dotenv()

def extract_city_data(df, city_name, lat, lon, country_code, state_code, timezone):
    city_data = {
        "city_name": city_name,
        "country_code": country_code,
        "data": [],
        "lat": lat,
        "lon": lon,
        "state_code": state_code,
        "timezone": timezone
    }
    
    for _, row in df.iterrows():
        data_entry = {
            "app_max_temp": row.get("app_max_temp"),
            "app_min_temp": row.get("app_min_temp"),
            "clouds": row.get("clouds"),
            "clouds_hi": row.get("clouds_hi"),
            "clouds_low": row.get("clouds_low"),
            "clouds_mid": row.get("clouds_mid"),
            "datetime": row.get("datetime"),
            "dewpt": row.get("dewpt"),
            "high_temp": row.get("high_temp"),
            "low_temp": row.get("low_temp"),
            "max_dhi": row.get("max_dhi"),
            "max_temp": row.get("max_temp"),
            "min_temp": row.get("min_temp"),
            "moon_phase": row.get("moon_phase"),
            "moon_phase_lunation": row.get("moon_phase_lunation"),
            "moonrise_ts": row.get("moonrise_ts"),
            "moonset_ts": row.get("moonset_ts"),
            "ozone": row.get("ozone"),
            "pop": row.get("pop"),
            "precip": row.get("precip"),
            "pres": row.get("pres"),
            "rh": row.get("rh"),
            "slp": row.get("slp"),
            "snow": row.get("snow"),
            "snow_depth": row.get("snow_depth"),
            "sunrise_ts": row.get("sunrise_ts"),
            "sunset_ts": row.get("sunset_ts"),
            "temp": row.get("temp"),
            "ts": row.get("ts"),
            "uv": row.get("uv"),
            "valid_date": row.get("valid_date"),
            "vis": row.get("vis"),
            "wind_cdir": row.get("wind_cdir"),
            "wind_cdir_full": row.get("wind_cdir_full"),
            "wind_dir": row.get("wind_dir"),
            "wind_gust_spd": row.get("wind_gust_spd"),
            "wind_spd": row.get("wind_spd")
        }
        city_data["data"].append(data_entry)
    
    return city_data

def send_to_kafka(topic, data, kafka_servers):
    producer = KafkaProducer(
        bootstrap_servers=kafka_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send(topic, data)
    producer.flush()
    producer.close()

if __name__ == "__main__":
    bucket_name = 'final-project-naya-college'  # Replace with your bucket name
    prefix = 'Weather_Data/'  # Directory prefix for the parquet files
    kafka_servers = ['localhost:9092']  # Replace with your Kafka server addresses
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
        parquet_files = minio_client.list_parquet_files(bucket_name, prefix)
        for object_name in parquet_files:
            print(f"Processing file: {object_name}")
            df = minio_client.read_parquet_file(bucket_name, object_name)
            for city, details in city_details.items():
                city_data = extract_city_data(df, city, **details)
                send_to_kafka(kafka_topic, city_data, kafka_servers)
    except Exception as e:
        print(f"Error: {e}")
