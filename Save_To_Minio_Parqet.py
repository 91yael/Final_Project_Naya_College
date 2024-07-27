import json
import requests
from minio import Minio
from minio.error import S3Error
import io
from datetime import datetime

# Your API key
api_key = '58dc65a39e0a47969faa150c8f380969'

# Base URL for the Weatherbit API
url = 'https://api.weatherbit.io/v2.0/forecast/daily'

# List of cities
cities = ["Rome", "Paris", "London", "New York", "Athens", "Barcelona", "Madrid", "Praha", "Budapest"]

# Dictionary to store the results
weather_data = {}

for city in cities:
    # Parameters for the API request
    params = {
        'key': api_key,
        'city': city,
        'days': 7  # Number of days for the forecast
    }
    
    # Make the GET request to the Weatherbit API
    response = requests.get(url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON data
        data = response.json()
        # Store the data in the dictionary
        weather_data[city] = data
    else:
        print(f"Error retrieving data for {city}: {response.status_code}")

# Connect to MinIO server
minio_client = Minio(
    'localhost:9001',  # MinIO server URL adjusted for your port mapping
    access_key='duA8EqnGDVkplqve',
    secret_key='N5uS3WcHV4V834Lan76KLp4A22eN1y2E',
    secure=False  # Set to True if using HTTPS
)

bucket_name = 'project'

# Get the current date
current_date = datetime.now().strftime('%Y-%m-%d')
file_path = f'Parqet/weather_forecast_{current_date}.parqet'

# Convert weather data to JSON string
weather_data_json = json.dumps(weather_data)

# Upload data to MinIO
try:
    # Check if the bucket exists
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    # Upload the JSON data as an object in the bucket
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=file_path,
        data=io.BytesIO(weather_data_json.encode('utf-8')),
        length=len(weather_data_json),
        content_type='application/json'
    )
    print(f"Weather data has been uploaded to {bucket_name}/{file_path} successfully.")
except S3Error as e:
    print(f"Error occurred: {e}")
