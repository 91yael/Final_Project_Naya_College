import json
import requests
from minio import Minio
from minio.error import S3Error
import io
import os
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from minio_utils import MinioClient

def main():
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv('WEATHER_API_KEY')
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

    current_date = datetime.now().strftime('%Y-%m-%d')
    parquet_filename = f'Weather_Data/weather_forecast_{current_date}.parquet'
    
    bucket_name = os.getenv('MINIO_BUCKET_NAME')
    

    # Create an instance of MinioClient
    minio_client = MinioClient()

    # Upload the DataFrame as a Parquet file to MinIO
    minio_client.upload_parquet_to_minio(bucket_name, parquet_filename, df)

if __name__ == "__main__":
    main()
