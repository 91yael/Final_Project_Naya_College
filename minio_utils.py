import os
import io
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error

class MinioClient:
    def __init__(self):
        self.minio_endpoint = os.getenv('MINIO_ENDPOINT')
        self.minio_access_key = os.getenv('MINIO_ACCESS_KEY')
        self.minio_secret_key = os.getenv('MINIO_SECRET_KEY')

        # Create a MinIO client
        self.client = Minio(
            self.minio_endpoint,
            access_key=self.minio_access_key,
            secret_key=self.minio_secret_key,
            secure=False  # Set to True if using HTTPS
        )

    def upload_to_minio(self, bucket_name, file_path, data, content_type='application/json'):
        try:
            # Check if the bucket exists
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)

            # Upload the data as an object in the bucket
            self.client.put_object(
                bucket_name=bucket_name,
                object_name=file_path,
                data=data,
                length=len(data.getvalue()),
                content_type=content_type
            )
            print(f"Data has been uploaded to {bucket_name}/{file_path} successfully.")
        except S3Error as e:
            print(f"Error occurred: {e}")

    def upload_json_to_minio(self, bucket_name, file_path, json_data):
        # Convert JSON data to BytesIO object
        json_buffer = io.BytesIO()
        json_buffer.write(json.dumps(json_data).encode('utf-8'))
        json_buffer.seek(0)

        # Upload the JSON data
        self.upload_to_minio(bucket_name, file_path, json_buffer, content_type='application/json')

    def upload_parquet_to_minio(self, bucket_name, file_path, dataframe):
        # Convert DataFrame to Parquet and save to a BytesIO object
        parquet_buffer = io.BytesIO()
        dataframe.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload the Parquet data
        self.upload_to_minio(bucket_name, file_path, parquet_buffer, content_type='application/octet-stream')

    def list_parquet_files(self, bucket_name, prefix):
        try:
            objects = self.client.list_objects(bucket_name, prefix=prefix, recursive=True)
            parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
            return parquet_files
        except S3Error as e:
            print(f"MinIO S3Error: {e}")
            raise

    def read_parquet_file(self, bucket_name, object_name):
        try:
            response = self.client.get_object(bucket_name, object_name)
            parquet_data = io.BytesIO(response.read())
            df = pd.read_parquet(parquet_data)
            response.close()
            response.release_conn()
        except S3Error as e:
            print(f"MinIO S3Error: {e}")
            raise
        return df

# Usage example:
# minio_client = MinioClient()
# minio_client.upload_json_to_minio(bucket_name, file_path, json_data)
# minio_client.upload_parquet_to_minio(bucket_name, file_path, dataframe)
# parquet_files = minio_client.list_parquet_files(bucket_name, prefix)
# df = minio_client.read_parquet_file(bucket_name, object_name)
