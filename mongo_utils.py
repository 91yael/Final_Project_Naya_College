import pymongo
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class MongoClientWrapper:
    def __init__(self):
        self.mongo_endpoint = os.getenv('MONGO_ENDPOINT')
        if not self.mongo_endpoint:
            raise ValueError("MONGO_ENDPOINT environment variable is not set")

        self.client = pymongo.MongoClient(self.mongo_endpoint)
        self.db = self.client['final_project']
        self.collection = self.db['weather_data']

    def insert_data_to_mongodb(self, data):
        try:
            self.collection.insert_one(data)
        except Exception as e:
            print(f"Error inserting data into MongoDB: {e}")

# Usage example:
# mongo_client = MongoClientWrapper()
# mongo_client.insert_data_to_mongodb(data)
