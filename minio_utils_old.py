import os
import io
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error

def upload_to_minio(bucket_name, file_path, data, content_type='application/json'):
    # Load MinIO environment variables
    minio_endpoint = os.getenv('MINIO_ENDPOINT')
    minio_access_key = os.getenv('MINIO_ACCESS_KEY')
    minio_secret_key = os.getenv('MINIO_SECRET_KEY')

    # Create a MinIO client
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to True if using HTTPS
    )

    try:
        # Check if the bucket exists
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

        # Upload the data as an object in the bucket
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=file_path,
            data=data,
            length=len(data.getvalue()),
            content_type=content_type
        )
        print(f"Data has been uploaded to {bucket_name}/{file_path} successfully.")
    except S3Error as e:
        print(f"Error occurred: {e}")

def upload_json_to_minio(bucket_name, file_path, json_data):
    # Convert JSON data to BytesIO object
    json_buffer = io.BytesIO()
    json_buffer.write(json.dumps(json_data).encode('utf-8'))
    json_buffer.seek(0)

    # Upload the JSON data
    upload_to_minio(bucket_name, file_path, json_buffer, content_type='application/json')

def upload_parquet_to_minio(bucket_name, file_path, dataframe):
    # Convert DataFrame to Parquet and save to a BytesIO object
    parquet_buffer = io.BytesIO()
    dataframe.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    # Upload the Parquet data
    upload_to_minio(bucket_name, file_path, parquet_buffer, content_type='application/octet-stream')
