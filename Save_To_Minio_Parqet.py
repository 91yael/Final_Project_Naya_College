import json
import requests
from minio import Minio
from minio.error import S3Error
import io
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

def main():
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv('Weather_api_key')
    if not api_key:
        raise ValueError("API key not found. Make sure it's set in the .env file.")

    # Base URL for the Weatherbit API
    url = 'https://api.weatherbit.io/v2.0/forecast/daily'

    # List of cities
    cities = ["Rome", "Paris", "London", "New York", "Athens", "Barcelona", "Madrid", "Praha", "Budapest"]

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

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(weather_data)

    # Convert the DataFrame to Parquet format
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Connect to MinIO server
    minio_client = Minio(
        'localhost:9001',  # MinIO server URL adjusted for your port mapping
        access_key=os.getenv("minio_access_key"),
        secret_key=os.getenv("minio_secret_key"),
        secure=False  # Set to True if using HTTPS
    )

    bucket_name = 'project'

    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    file_path = f'Parquet/weather_forecast_{current_date}.parquet'

    # Upload data to MinIO
    try:
        # Check if the bucket exists
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
        
        # Upload the Parquet data as an object in the bucket
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=file_path,
            data=io.BytesIO(parquet_buffer.getvalue()),
            length=parquet_buffer.getbuffer().nbytes,
            content_type='application/octet-stream'
        )
        print(f"Weather data has been uploaded to {bucket_name}/{file_path} successfully.")
    except S3Error as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
