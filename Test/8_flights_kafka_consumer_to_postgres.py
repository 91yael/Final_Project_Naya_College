import os
import json
import signal
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from kafka import KafkaConsumer

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
    return psycopg2.connect(
        dbname=dbname,
        host=postgres_host,
        port=postgres_port,
        user=postgres_user,
        password=postgres_password
    )

def create_database_if_not_exists():
    conn = connect_postgres()
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(
            sql.Identifier(postgres_db)
        ))
    except psycopg2.errors.DuplicateDatabase:
        pass
    conn.close()

def create_table_if_not_exists():
    conn = connect_postgres(dbname=postgres_db)
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS flights_data (
        id SERIAL PRIMARY KEY,
        from_city VARCHAR,
        to_city VARCHAR,
        depart_date DATE,
        return_date DATE,
        price_dollar FLOAT,
        outbound_leg_id VARCHAR,
        outbound_leg_departure_time TIMESTAMP,
        outbound_leg_arrival_time TIMESTAMP,
        outbound_leg_origin_airport VARCHAR,
        outbound_leg_destination_airport VARCHAR,
        outbound_leg_flight_number VARCHAR,
        outbound_leg_airline VARCHAR,
        return_leg_id VARCHAR,
        return_leg_departure_time TIMESTAMP,
        return_leg_arrival_time TIMESTAMP,
        return_leg_origin_airport VARCHAR,
        return_leg_destination_airport VARCHAR,
        return_leg_flight_number VARCHAR,
        return_leg_airline VARCHAR, 
        roundtrip_id VARCHAR,
        CONSTRAINT unique_flight UNIQUE (outbound_leg_id, return_leg_id, depart_date)
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
                        INSERT INTO flights_data (
                        from_city, 
                        to_city, 
                        depart_date, 
                        return_date, 
                        price_dollar, 
                        outbound_leg_id, 
                        outbound_leg_departure_time, 
                        outbound_leg_arrival_time, 
                        outbound_leg_origin_airport, 
                        outbound_leg_destination_airport, 
                        outbound_leg_flight_number, 
                        outbound_leg_airline, 
                        return_leg_id, 
                        return_leg_departure_time, 
                        return_leg_arrival_time, 
                        return_leg_origin_airport, 
                        return_leg_destination_airport, 
                        return_leg_flight_number, 
                        return_leg_airline, 
                        roundtrip_id
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (outbound_leg_id, return_leg_id, depart_date)
                    DO UPDATE SET
                        from_city = EXCLUDED.from_city,
                        to_city = EXCLUDED.to_city,
                        return_date = EXCLUDED.return_date,
                        price_dollar = EXCLUDED.price_dollar,
                        outbound_leg_departure_time = EXCLUDED.outbound_leg_departure_time,
                        outbound_leg_arrival_time = EXCLUDED.outbound_leg_arrival_time,
                        outbound_leg_origin_airport = EXCLUDED.outbound_leg_origin_airport,
                        outbound_leg_destination_airport = EXCLUDED.outbound_leg_destination_airport,
                        outbound_leg_flight_number = EXCLUDED.outbound_leg_flight_number,
                        outbound_leg_airline = EXCLUDED.outbound_leg_airline,
                        return_leg_departure_time = EXCLUDED.return_leg_departure_time,
                        return_leg_arrival_time = EXCLUDED.return_leg_arrival_time,
                        return_leg_origin_airport = EXCLUDED.return_leg_origin_airport,
                        return_leg_destination_airport = EXCLUDED.return_leg_destination_airport,
                        return_leg_flight_number = EXCLUDED.return_leg_flight_number,
                        return_leg_airline = EXCLUDED.return_leg_airline,
                        roundtrip_id = EXCLUDED.roundtrip_id;
                            """

        cursor.execute(insert_query, (
            record.get('from_city'),
            record.get('to_city'),
            record.get('depart_date'),
            record.get('return_date'),
            record.get('price_dollar'),
            record.get('outbound_leg', {}).get('id'),
            record.get('outbound_leg', {}).get('departure_time'),
            record.get('outbound_leg', {}).get('arrival_time'),
            record.get('outbound_leg', {}).get('origin_airport'),
            record.get('outbound_leg', {}).get('destination_airport'),
            record.get('outbound_leg', {}).get('flight_number'),
            record.get('outbound_leg', {}).get('airline'),
            record.get('return_leg', {}).get('id'),
            record.get('return_leg', {}).get('departure_time'),
            record.get('return_leg', {}).get('arrival_time'),
            record.get('return_leg', {}).get('origin_airport'),
            record.get('return_leg', {}).get('destination_airport'),
            record.get('return_leg', {}).get('flight_number'),
            record.get('return_leg', {}).get('airline'),
            record.get('roundtrip_id')
        ))
        
        conn.commit()
        conn.close()
        print(f"Record inserted")
    except Exception as e:
        print(f"Error inserting record: {e}")

def signal_handler(sig, frame):
    consumer.close()
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    create_database_if_not_exists()
    create_table_if_not_exists()

    for message in consumer:
        record = json.loads(message.value.decode('utf-8'))
        insert_record(record)

if __name__ == "__main__":
    main()