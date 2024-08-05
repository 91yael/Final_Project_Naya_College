import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from flight_utils import FlightUtils

def load_destinations(file_path):
    """Load destinations from a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)

def clean_flight_data(flight_data):
    """Clean flight data to remove empty entries."""
    cleaned_data = []
    for flight in flight_data:
        if isinstance(flight, dict) and all(isinstance(v, (str, int, float, list, dict)) for v in flight.values()):
            cleaned_data.append({k: (v if v != {} else None) for k, v in flight.items()})
    return cleaned_data

def send_to_kafka(producer, topic, data):
    """Send data to Kafka topic."""
    for record in data:
        producer.send(topic, json.dumps(record).encode('utf-8'))

def get_top_3_cheapest_flights(flights_data):
    """Extract top 3 cheapest flights."""
    flights = []
    itineraries = flights_data.get("data", {}).get("itineraries", [])

    for itinerary in itineraries:
        price_raw = itinerary.get("price", {}).get("raw", float('inf'))
        flights.append({
            "id": itinerary.get("id"),
            "price_raw": price_raw,
            "itinerary": itinerary
        })

    # Sort flights by price
    flights_sorted = sorted(flights, key=lambda x: x["price_raw"])

    # Get top 3 cheapest flights
    return flights_sorted[:3]

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
        bootstrap_servers=[kafka_broker],
        retries=5,
        request_timeout_ms=30000,  # Increase request timeout
        batch_size=16384,  # Increase batch size
        linger_ms=5000,  # Increase linger time
        acks='all'  # Ensure message is acknowledged by all in-sync replicas
    )

    # Load destinations
    destinations = load_destinations('destinations.json')
    weekends = flight_utils.get_next_weekends()

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
                
                # Get top 3 cheapest flights
                top_3_flights = get_top_3_cheapest_flights(flights_data)

                # Prepare messages
                messages = []
                for flight in top_3_flights:
                    messages.append({
                        "from_city": from_city,
                        "to_city": to_city,
                        "depart_date": depart_date,
                        "return_date": return_date,
                        "price_raw": flight["price_raw"]
                    })

                send_to_kafka(producer, kafka_topic, messages)

            except Exception as e:
                print(f"Error processing flight data: {str(e)}")

    # Ensure all messages are sent
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
