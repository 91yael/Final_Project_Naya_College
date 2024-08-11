import os
import json
from dotenv import load_dotenv
from kafka import KafkaProducer
from flight_utils import FlightUtils

#Send data to Kafka topic
def send_to_kafka(producer, topic, data):
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
    destinations = flight_utils.load_destinations(flight_utils.destinations_path)
    # Get next weekends
    weekends = flight_utils.get_next_weekends(flight_utils.num_weekends_to_check)
    # Ge next workdays
    workdays = flight_utils.get_next_workdays(flight_utils.num_weeks_to_check)
    # Weekends and workdays
    all_dates = weekends + workdays

    for destination in destinations:
        from_city = destination.get("from")
        to_city = destination.get("to")
        if not from_city or not to_city:
            print("Missing 'from' or 'to' key in destination.")
            continue
        
        for depart_date, return_date in all_dates:
            try:
                print(f"Fetching location ID for {from_city} and {to_city}...")
                from_id = flight_utils.get_location_id(from_city, headers)
                to_id = flight_utils.get_location_id(to_city, headers)
                
                flights_data = flight_utils.get_flights(from_id, to_id, depart_date, return_date, headers)
                
                # Clean flight data to include only direct flights
                clean_data, all_itineraries = flight_utils.clean_flight_data(flights_data)
                
                # Extract necessary flight details
                detailed_flights = flight_utils.extract_flight_details(clean_data)
                
                # Get top cheapest flights
                top_cheapest_flights = flight_utils.get_top_cheapest_flights(detailed_flights, flight_utils.num_cheapest_flights)
                # Prepare flight_messages
                flight_messages = []
                for flight in top_cheapest_flights:
                    flight_messages.append({
                        "roundtrip_id": flight["id"],
                        "from_country": flight["outbound_leg"]["origin_airport"],
                        "to_country": flight["outbound_leg"]["destination_airport"],
                        "from_city": from_city,
                        "to_city": to_city,
                        "depart_date": depart_date,
                        "return_date": return_date,
                        "price_dollar": flight["price_dollar"],
                        "outbound_leg": {
                            "id": flight["outbound_leg"]["id"],
                            "departure_time": flight["outbound_leg"]["departure_time"],
                            "arrival_time": flight["outbound_leg"]["arrival_time"],
                            "origin_airport": flight["outbound_leg"]["origin_airport"],
                            "destination_airport": flight["outbound_leg"]["destination_airport"],
                            "flight_number": flight["outbound_leg"]["flight_number"],
                            "airline": flight["outbound_leg"].get("airline")
                        },
                        "return_leg": {
                            "id": flight["return_leg"]["id"],
                            "departure_time": flight["return_leg"]["departure_time"],
                            "arrival_time": flight["return_leg"]["arrival_time"],
                            "origin_airport": flight["return_leg"]["origin_airport"],
                            "destination_airport": flight["return_leg"]["destination_airport"],
                            "flight_number": flight["return_leg"]["flight_number"],
                            "airline": flight["return_leg"].get("airline")
                        }
                    })
                
                # Send flight messages
                send_to_kafka(producer, kafka_topic, flight_messages)
                
                # Prepare and send itineraries message with prefix
                itineraries_message = {
                    "Full data:"
                    "itineraries": all_itineraries
                }
                send_to_kafka(producer, kafka_topic, [itineraries_message])

                print("Messages sent successfully")

            except Exception as e:
                print(f"Error processing flight data: {str(e)}")

    # Ensure all messages are sent
    producer.flush()
    producer.close()
    
if __name__ == "__main__":
    main()
    