import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from flight_utils import FlightUtils

# Load environment variables from .env file
load_dotenv('.env')
api_key = os.getenv('RAPIDAPI_KEY')
kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = os.getenv('KAFKA_TOPIC_FLIGHTS_3')

if not api_key:
    raise ValueError("API key not found. Make sure it's set in the .env file.")

# Initialize FlightUtils
flight_utils = FlightUtils()

headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": flight_utils.rapidapi_host
}

def send_to_kafka(producer, topic, data):
    for record in data:
        summary = {
            "from_city": record["from_city"],
            "to_city": record["to_city"],
            "depart_date": record["depart_date"],
            "return_date": record["return_date"],
            "price_raw": record["price_raw"]
        }
        print(f"Sending record to Kafka: {summary}") 
        future = producer.send(topic, json.dumps(record).encode('utf-8'))
        try:
            future.get(timeout=30)  # Wait for the message to be acknowledged
            print("Message sent successfully.")
        except Exception as e:
            print(f"Failed to send message to Kafka: {e}")

def get_top_3_cheapest_flights(flights_data):
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
    weekends = flight_utils.get_next_weekends()
    destinations = flight_utils.load_destinations('destinations.json')

    producer = KafkaProducer(
    bootstrap_servers=[kafka_broker],
    retries=5,
    request_timeout_ms=30000,  # Increase request timeout
    batch_size=16384,  # Increase batch size
    linger_ms=5000,  # Increase linger time
    acks='all'  # Ensure message is acknowledged by all in-sync replicas
    )

    producer = KafkaProducer(bootstrap_servers=[kafka_broker], retries=5)
    
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
                        #"itinerary": flight["itinerary"] 
                    })
                
                send_to_kafka(producer, kafka_topic, messages)

            except Exception as e:
                print(f"Error processing flight data: {str(e)}")

    producer.close()

if __name__ == "__main__":
    main()
