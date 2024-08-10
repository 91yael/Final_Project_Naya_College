import os
import json
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from kafka import KafkaConsumer
from psycopg2 import sql

# Load environment variables from .env file
load_dotenv()

# Initialize parameters
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC_FLIGHTS_4')
postgres_host = os.getenv('POSTGRES_HOST')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_db = os.getenv('POSTGRES_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset='earliest',
    group_id='my-group'
)

# Connect to PostgreSQL
def connect_postgres(dbname=None):
    conn = psycopg2.connect(
        dbname=dbname,
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password
    )
    return conn

def create_database_if_not_exists():
    conn = connect_postgres()
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(postgres_db)
        ))
        print(f"Database {postgres_db} created.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database {postgres_db} already exists.")
    conn.close()

def create_table_if_not_exists():
    conn = connect_postgres(dbname=postgres_db)
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS flights (
        id SERIAL PRIMARY KEY,
        from_city VARCHAR,
        to_city VARCHAR,
        depart_date DATE,
        return_date DATE,
        price_raw FLOAT,
        itinerary JSONB
    )
    """
    cursor.execute(create_table_query)
    conn.commit()
    conn.close()

def insert_record(record):
    try:
        conn = connect_postgres(dbname=postgres_db)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO flights (from_city, to_city, depart_date, return_date, price_raw, itinerary)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            record.get('from_city'),
            record.get('to_city'),
            record.get('depart_date'),
            record.get('return_date'),
            record.get('price_raw'),
            json.dumps(record.get('itinerary'))  # Convert dict to JSON string
        ))
        conn.commit()
        conn.close()
        print(f"Inserted record into PostgreSQL: {record}")
    except Exception as e:
        print(f"Error inserting record into PostgreSQL: {e}")

def main():
    create_database_if_not_exists()
    create_table_if_not_exists()

    for message in consumer:
        record = json.loads(message.value.decode('utf-8'))
        insert_record(record)

if __name__ == "__main__":
    main()
