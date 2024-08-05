import os
import json
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
from flight_main import get_location_id, get_flights, get_next_weekends, clean_flight_data, rapidapi_host

# Load environment variables
load_dotenv('.env')

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
KAFKA_TOPIC_FLIGHTS_3 = os.getenv('KAFKA_TOPIC_FLIGHTS_3')

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_flights_to_kafka(destination, weekends, headers):
    from_city = destination["from"]
    to_city = destination["to"]
    to_country = destination.get("country", "unknown").replace(" ", "_")
    
    for depart_date, return_date in weekends:
        try:
            from_id = get_location_id(from_city, headers)
            to_id = get_location_id(to_city, headers)
            flights = get_flights(from_id, to_id, depart_date, return_date, headers)
            cleaned_flights = clean_flight_data(flights)
            flight_data = {
                "from_city": from_city,
                "to_city": to_city,
                "depart_date": depart_date,
                "return_date": return_date,
                "flight_data": cleaned_flights
            }

            # Produce message to Kafka
            producer.produce(KAFKA_TOPIC_FLIGHTS_3, key=json.dumps(flight_data), value=json.dumps(flight_data), callback=delivery_report)
            producer.flush()
        except Exception as e:
            logging.error(f"Error processing flights for {from_city} to {to_city} on {depart_date}: {e}")

def load_destinations(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def main():
    api_key = os.getenv('RAPIDAPI_KEY')
    if not api_key:
        raise ValueError("API key not found. Make sure it's set in the .env file.")
    
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": rapidapi_host
    }
    
    destinations = load_destinations('destinations.json')
    weekends = get_next_weekends()

    for destination in destinations:
        send_flights_to_kafka(destination, weekends, headers)

if __name__ == "__main__":
    main()
