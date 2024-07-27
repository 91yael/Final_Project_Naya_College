import requests

# Your API key
api_key = '58dc65a39e0a47969faa150c8f380969'

# Base URL for the Weatherbit API
url = 'https://api.weatherbit.io/v2.0/forecast/daily'

# List of cities
cities = ["Rome", "Paris", "London", "New York", "Athens", "Barcelona", "Madrid", "Praha", "Budapest"]

# Dictionary to store the results
weather_data = {}

for city in cities:
    # Parameters for the API request
    params = {
        'key': api_key,
        'city': city,
        'days': 7  # Number of days for the forecast
    }
    
    # Make the GET request to the Weatherbit API
    response = requests.get(url, params=params)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the JSON data
        data = response.json()
        # Store the data in the dictionary
        weather_data[city] = data
    else:
        print(f"Error retrieving data for {city}: {response.status_code}")

# Print the data for each city (or process it as needed)
for city, data in weather_data.items():
    print(f"Weather forecast for {city}:")
    print(data)
    print("\n")
