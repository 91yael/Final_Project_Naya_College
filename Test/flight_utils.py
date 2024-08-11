import requests
import os
from datetime import datetime, timedelta
import json

class FlightUtils:
    def __init__(self):
        """Initialize Variables"""
        self.url_location = "https://skyscanner80.p.rapidapi.com/api/v1/flights/auto-complete"
        self.url_flights = "https://skyscanner80.p.rapidapi.com/api/v1/flights/search-roundtrip"
        self.rapidapi_host = os.getenv('RAPIDAPI_HOST', "skyscanner80.p.rapidapi.com")

        #The number of weekends to retrieve
        self.num_weekends_to_check = 1

        #The number of weeks to retrieve
        self.num_weeks_to_check = 1

        #The number of top cheapest flights to retrieve
        self.num_cheapest_flights = 1

        #The path of destinations file.
        self.destinations_path = 'destinations.json'

        """"""

    # Get location ID
    def get_location_id(self, query, headers):
        response = requests.get(self.url_location, headers=headers, params={"query": query})
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}, {response.text}")
        data = response.json()

        if 'data' in data and len(data['data']) > 0:
            return data['data'][0]['id']
        else:
            raise ValueError("Location ID not found in the response")

    # Get flights
    def get_flights(self, from_id, to_id, depart_date, return_date, headers):
        querystring = {
            "fromId": from_id,
            "toId": to_id,
            "departDate": depart_date,
            "returnDate": return_date,
            "adults": "1",
            "cabinClass": "economy",
            "currency": "USD",
            "market": "US",
            "locale": "en-US"
        }
        response = requests.get(self.url_flights, headers=headers, params=querystring)
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code}, {response.text}")
        return response.json()
    
    # Clean flight data to include only direct flights
    def clean_flight_data(self, flight_data):
        cleaned_data = []
        itineraries = {}
        for flight in flight_data.get("data", {}).get("itineraries", []):
            if isinstance(flight, dict):
                stop_count = flight.get("stopCount", 0)
                if stop_count == 0:
                    flight_id = flight.get("id")
                    cleaned_data.append({k: (v if v != {} else None) for k, v in flight.items()})
                    itineraries[flight_id] = flight
        return cleaned_data, itineraries
    
    # Extract necessary fields
    def extract_flight_details(self, flight_data):
        detailed_flights = []
        for flight in flight_data:
            # Extracting details for the outbound leg
            outbound_leg = flight.get("legs", [])[0] if len(flight.get("legs", [])) > 0 else {}
            outbound_leg_id = outbound_leg.get("id")
            outbound_departure = outbound_leg.get("departure")
            outbound_arrival = outbound_leg.get("arrival")
            outbound_origin = outbound_leg.get("origin", {})
            outbound_destination = outbound_leg.get("destination", {})
            outbound_flight_number = None
            outbound_airline = None

            for segment in outbound_leg.get("segments", []):
                if segment.get("origin", {}).get("country") == outbound_origin.get("country"):
                    outbound_flight_number = segment.get("flightNumber", "")
            
            for carrier in outbound_leg.get("carriers", {}).get("marketing", []):
                if carrier.get("name"):
                    outbound_airline = carrier.get("name")

            # Extracting details for the return leg
            return_leg = flight.get("legs", [])[1] if len(flight.get("legs", [])) > 1 else {}
            return_leg_id = return_leg.get("id")
            return_departure = return_leg.get("departure")
            return_arrival = return_leg.get("arrival")
            return_origin = return_leg.get("origin", {})
            return_destination = return_leg.get("destination", {})
            return_flight_number = None
            return_airline = None

            for segment in return_leg.get("segments", []):
                if segment.get("origin", {}).get("country") == return_origin.get("country"):
                    return_flight_number = segment.get("flightNumber", "")

            for carrier in return_leg.get("carriers", {}).get("marketing", []):
                if carrier.get("name"):
                    return_airline = carrier.get("name")

            detailed_flights.append({
                "id": flight.get("id"),
                "price_dollar": flight.get("price", {}).get("raw", float('inf')),
                "outbound_leg": {
                    "id": outbound_leg_id,
                    "departure_time": outbound_departure,
                    "arrival_time": outbound_arrival,
                    "origin_airport": outbound_origin.get("name"),
                    "destination_airport": outbound_destination.get("name"),
                    "flight_number": outbound_flight_number,
                    "airline": outbound_airline
                },
                "return_leg": {
                    "id": return_leg_id,
                    "departure_time": return_departure,
                    "arrival_time": return_arrival,
                    "origin_airport": return_origin.get("name"),
                    "destination_airport": return_destination.get("name"),
                    "flight_number": return_flight_number,
                    "airline": return_airline 
                }
            })
        return detailed_flights


    
    # Extract top cheapest flights
    def get_top_cheapest_flights(self, flights_data, top_cheapest_flights):
        flights_sorted = sorted(flights_data, key=lambda x: x["price_dollar"])
        return flights_sorted[:top_cheapest_flights]
    

    # Get next weekends
    def get_next_weekends(self, num_weekends=None):
        if num_weekends is None:
            num_weekends = self.num_weekends_to_check
        today = datetime.now()
        weekends = []
        for i in range(num_weekends):
            start_of_weekend = today + timedelta((3 - today.weekday() + 7) % 7 + i * 7)  # Thursday
            end_of_weekend = start_of_weekend + timedelta(2)  # Saturday
            weekends.append((start_of_weekend.strftime("%Y-%m-%d"), end_of_weekend.strftime("%Y-%m-%d")))
        return weekends
    
    # Get next workdays
    def get_next_workdays(self, num_weeks=None):
        if num_weeks is None:
            num_weeks = self.num_weeks_to_check
        today = datetime.now()
        workdays = []
        for i in range(num_weeks):
            start_of_week = today + timedelta((6 - today.weekday() + 7) % 7 + i * 7)  # Next Sunday
            end_of_week = start_of_week + timedelta(4)  # Next Thursday
            workdays.append((start_of_week.strftime("%Y-%m-%d"), end_of_week.strftime("%Y-%m-%d")))
        return workdays
    
    # Load destinations
    def load_destinations(self, file_path):
        with open(file_path, 'r') as file:
            return json.load(file)
