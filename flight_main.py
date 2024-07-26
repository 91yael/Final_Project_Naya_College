import os
import json
import pandas as pd
from dotenv import load_dotenv
from flight_utils import get_location_id, get_flights, get_next_weekends, rapidapi_host, num_weekends_to_check

def load_destinations(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def main():
    # Load environment variables from .env file
    load_dotenv()
    api_key = os.getenv('RAPIDAPI_KEY')
    if not api_key:
        raise ValueError("API key not found. Make sure it's set in the .env file.")
    
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": rapidapi_host
    }
    
    # Load the list of destinations
    destinations = load_destinations('destinations.json')
    
    weekends = get_next_weekends()
    
    for destination in destinations:
        from_city = destination["from"]
        to_city = destination["to"]
        to_country = destination.get("country", "unknown").replace(" ", "_")  
        
        for depart_date, return_date in weekends:
            all_flights = []
            
            from_id = get_location_id(from_city, headers)
            to_id = get_location_id(to_city, headers)
            
            flights = get_flights(from_id, to_id, depart_date, return_date, headers)
            all_flights.append({
                "from_city": from_city,
                "to_city": to_city,
                "depart_date": depart_date,
                "return_date": return_date,
                "flight_data": flights 
            })
            
            # Save as Parquet file
            df = pd.DataFrame(all_flights)
            parquet_filename = f'flights_data_{depart_date}_to_{return_date}_{to_country}.parquet'
            df.to_parquet(parquet_filename, index=False)
            
            # Print confirmation
            print(f"Flight data has been saved to '{parquet_filename}'.")

if __name__ == "__main__":
    main()
