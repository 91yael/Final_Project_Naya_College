import requests
from datetime import datetime, timedelta

# Define variables
url_location = "https://skyscanner80.p.rapidapi.com/api/v1/flights/auto-complete"
url_flights = "https://skyscanner80.p.rapidapi.com/api/v1/flights/search-roundtrip"
rapidapi_host = "skyscanner80.p.rapidapi.com"
num_weekends_to_check = 2

def get_location_id(query, headers):
    response = requests.get(url_location, headers=headers, params={"query": query})
    if response.status_code != 200:
        raise ValueError(f"Error: {response.status_code}, {response.text}")
    data = response.json()
    
    if 'data' in data and len(data['data']) > 0:
        return data['data'][0]['id']
    else:
        raise ValueError("Location ID not found in the response")

def get_flights(from_id, to_id, depart_date, return_date, headers):
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
    response = requests.get(url_flights, headers=headers, params=querystring)
    if response.status_code != 200:
        raise ValueError(f"Error: {response.status_code}, {response.text}")
    return response.json()

def get_next_weekends(num_weekends=num_weekends_to_check):
    today = datetime.now()
    weekends = []
    for i in range(num_weekends):
        start_of_weekend = today + timedelta((3 - today.weekday() + 7) % 7 + i * 7)  # Thursday
        end_of_weekend = start_of_weekend + timedelta(2)  # Saturday
        weekends.append((start_of_weekend.strftime("%Y-%m-%d"), end_of_weekend.strftime("%Y-%m-%d")))
    return weekends
