import json
import os
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

def extract_city_weather_data(weather_data):
    city_weather_info = {}

    for city, details in weather_data.items():
        if isinstance(details, dict) and "city_name" in details:
            city_info = {
                "city_name": details.get("city_name", ""),
                "country_code": details.get("country_code", ""),
                "latitude": details.get("lat", 0),
                "longitude": details.get("lon", 0),
                "state_code": details.get("state_code", ""),
                "timezone": details.get("timezone", ""),
                "forecast": []
            }

            for day in details.get("data", []):
                if isinstance(day, dict):
                    day_info = {
                        "date": day.get("datetime", ""),
                        "temperature": {
                            "high_temp": day.get("high_temp", 0),
                            "low_temp": day.get("low_temp", 0),
                            "average_temp": day.get("temp", 0)
                        },
                        "precipitation": day.get("precip", 0),
                        "humidity": day.get("rh", 0),
                        "wind": {
                            "speed": day.get("wind_spd", 0),
                            "direction": day.get("wind_cdir", ""),
                            "gust_speed": day.get("wind_gust_spd", 0)
                        },
                        "weather": {
                            "description": day.get("weather", {}).get("description", ""),
                            "icon": day.get("weather", {}).get("icon", "")
                        },
                        "uv_index": day.get("uv", 0),
                        "visibility": day.get("vis", 0)
                    }
                    city_info["forecast"].append(day_info)

            city_weather_info[city] = city_info
        else:
            print(f"Unexpected data format for city {city}: {details}")

    return city_weather_info

def insert_data_to_postgres(conn, data):
    try:
        cursor = conn.cursor()

        insert_weather_query = """
        INSERT INTO weather_data (city_name, country_code, latitude, longitude, state_code, timezone)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """

        insert_forecast_query = """
        INSERT INTO forecast_data (weather_data_id, date, high_temp, low_temp, average_temp, precipitation, humidity, wind_speed, wind_direction, gust_speed, weather_description, weather_icon, uv_index, visibility)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for city, details in data.items():
            cursor.execute(insert_weather_query, (
                details['city_name'],
                details['country_code'],
                float(details['latitude'] or 0),  # Default to 0 if None
                float(details['longitude'] or 0),  # Default to 0 if None
                details['state_code'],
                details['timezone']
            ))
            weather_data_id = cursor.fetchone()[0]

            for forecast in details['forecast']:
                cursor.execute(insert_forecast_query, (
                    weather_data_id,
                    forecast['date'],
                    float(forecast['temperature']['high_temp'] or 0),  # Default to 0 if None
                    float(forecast['temperature']['low_temp'] or 0),  # Default to 0 if None
                    float(forecast['temperature']['average_temp'] or 0),  # Default to 0 if None
                    float(forecast['precipitation'] or 0),  # Default to 0 if None
                    int(forecast['humidity'] or 0),  # Default to 0 if None
                    float(forecast['wind']['speed'] or 0),  # Default to 0 if None
                    forecast['wind']['direction'],
                    float(forecast['wind']['gust_speed'] or 0),  # Default to 0 if None
                    forecast['weather']['description'],
                    forecast['weather']['icon'],
                    int(forecast['uv_index'] or 0),  # Default to 0 if None
                    float(forecast['visibility'] or 0)  # Default to 0 if None
                ))

        conn.commit()
        cursor.close()
    except Exception as err:
        print(f"An error occurred while inserting data into PostgreSQL: {err}")

def main():
    # Load environment variables from .env file
    load_dotenv()

    kafka_broker = os.getenv('KAFKA_BROKER')
    kafka_topic = os.getenv('KAFKA_TOPIC_WEATHER_2')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT', 5432)  # Default to 5432 if not set
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')

    if not kafka_broker or not kafka_topic or not postgres_host or not postgres_port or not postgres_db or not postgres_user or not postgres_password:
        raise ValueError("Kafka or PostgreSQL configuration not found. Make sure it's set in the .env file.")
    
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=[kafka_broker],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather_data_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )

    for message in consumer:
        weather_data = message.value
        city_weather_data = extract_city_weather_data(weather_data)
        insert_data_to_postgres(conn, city_weather_data)
    
    conn.close()

if __name__ == "__main__":
    main()
