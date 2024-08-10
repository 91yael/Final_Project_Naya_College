import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from Test.flight_utils import FlightUtils

"""Initialize parameters"""

#The number of top cheapest flights to retrieve.
num_cheapest_flights=3

#The number of weekends to retrieve.
next_weekends=2

#The path of destinations file.
destination_path = 'destinations.json'

""""""

#Send data to Kafka topic
def send_to_kafka(producer, topic, data):
    print(f"Sending {len(data)} records to Kafka topic '{topic}'...")
    print(f"data: {data}")
    for record in data:
        producer.send(topic, json.dumps(record).encode('utf-8'))

def main():
    # Load environment variables from .env file
    load_dotenv()
    
    api_key = os.getenv('RAPIDAPI_KEY')
    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_topic = os.getenv('KAFKA_TOPIC_FLIGHTS_3')

    if not api_key or not kafka_broker or not kafka_topic:
        raise ValueError("API key, Kafka broker, or topic not found. Make sure they are set in the .env file.")
    
    # Initialize FlightUtils
    flight_utils = FlightUtils()

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": flight_utils.rapidapi_host
    }

    # Kafka producer setup
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',
        retries=5,
        request_timeout_ms=30000,  
        batch_size=16384,  
        linger_ms=5000, 
        acks='all'  
    )

    # Load destinations
    destinations = flight_utils.load_destinations(destination_path)
    # Get next weekends
    weekends = flight_utils.get_next_weekends(next_weekends)

    for destination in destinations:
        from_city = destination.get("from")
        to_city = destination.get("to")
        if not from_city or not to_city:
            print("Missing 'from' or 'to' key in destination.")
            continue
        
        for depart_date, return_date in weekends:
            try:
                print(f"Fetching location ID for {from_city} and {to_city}...")
                from_id = flight_utils.get_location_id(from_city, headers)
                to_id = flight_utils.get_location_id(to_city, headers)
                
                print(f"Fetching flights from {from_id} to {to_id} for dates {depart_date} to {return_date}...")
                flights_data = flight_utils.get_flights(from_id, to_id, depart_date, return_date, headers)
                
                # Get top cheapest flights
                top_cheapest_flights = flight_utils.get_top_cheapest_flights(flights_data,num_cheapest_flights)

                # Prepare messages
                messages = []
                for flight in top_cheapest_flights:
                    messages.append({
                        "from_city": from_city,
                        "to_city": to_city,
                        "depart_date": depart_date,
                        "return_date": return_date,
                        "price_raw": flight["price_raw"],
                        "itinerary": flight["itinerary"] 
                    })
                print(f"messages: {messages}")
                print(f"producer: {producer}, kafka_topic: {kafka_topic}")
                send_to_kafka(producer, kafka_topic, messages)

            except Exception as e:
                print(f"Error processing flight data: {str(e)}")

    # Ensure all messages are sent
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
