import json
import os
import psycopg2
import pyarrow.parquet as pq
import pyarrow as pa
from kafka import KafkaConsumer
from dotenv import load_dotenv
import io

def extract_and_insert_data(conn, weather_data):
    try:
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO weather_forecast (
            city_name, country_code, latitude, longitude, state_code, timezone,
            date, high_temp, low_temp, average_temp, precipitation, humidity,
            wind_speed, wind_direction, gust_speed, weather_description, weather_icon,
            uv_index, visibility
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city_name, date) DO UPDATE SET
            high_temp = EXCLUDED.high_temp,
            low_temp = EXCLUDED.low_temp,
            average_temp = EXCLUDED.average_temp,
            precipitation = EXCLUDED.precipitation,
            humidity = EXCLUDED.humidity,
            wind_speed = EXCLUDED.wind_speed,
            wind_direction = EXCLUDED.wind_direction,
            gust_speed = EXCLUDED.gust_speed,
            weather_description = EXCLUDED.weather_description,
            weather_icon = EXCLUDED.weather_icon,
            uv_index = EXCLUDED.uv_index,
            visibility = EXCLUDED.visibility
        """

        # print(f"Type of weather_data: {type(weather_data)}")
        if isinstance(weather_data, dict):
            print(f"Keys of weather_data: {list(weather_data.keys())}")
            weather_data = [weather_data]  

        print(f"First few records: {weather_data[:5]}")  

        for record in weather_data:
            if isinstance(record, dict):
                city_name = record.get("city", "")
                country_code = ""  
                latitude = 0.0  
                longitude = 0.0  
                state_code = ""  
                timezone = "" 

                date = record.get("datetime", "")
                high_temp = float(record.get("high_temp", 0))
                low_temp = float(record.get("low_temp", 0))
                average_temp = float(record.get("temp", 0))
                precipitation = float(record.get("precip", 0))
                humidity = int(record.get("rh", 0))
                wind_speed = float(record.get("wind_spd", 0))
                wind_direction = record.get("wind_cdir", "")
                gust_speed = float(record.get("wind_gust_spd", 0))
                weather_description = record.get("weather", {}).get("description", "")
                weather_icon = record.get("weather", {}).get("icon", "")
                uv_index = int(record.get("uv", 0))
                visibility = float(record.get("vis", 0))

                print(f"Inserting data for {city_name} on {date}")

                try:
                    cursor.execute(insert_query, (
                        city_name, country_code, latitude, longitude, state_code, timezone,
                        date, high_temp, low_temp, average_temp, precipitation, humidity,
                        wind_speed, wind_direction, gust_speed, weather_description, weather_icon,
                        uv_index, visibility
                    ))
                except Exception as e:
                    print(f"Error executing insert for {city_name} on {date}: {e}")

        conn.commit()
        cursor.close()
    except Exception as err:
        print(f"An error occurred while inserting data into PostgreSQL: {err}")
        conn.rollback()

def main():

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
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  
    )

    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password
    )

    print("Starting to consume messages...")

    for message in consumer:
        print("Received message")

        try:
            
            message_data = message.value
            print("Message is in JSON format")
            print(f"Processing message with {len(message_data)} records")
            extract_and_insert_data(conn, message_data)
        except json.JSONDecodeError:
            print("Message is not in JSON format, attempting to read as Parquet")
            try:
                parquet_buffer = io.BytesIO(message.value)
                table = pq.read_table(parquet_buffer)
                df = table.to_pandas()
                
                # Convert the dataframe to a list of dictionaries
                weather_data = df.to_dict(orient='records')
                
                print(f"Processing message with {len(weather_data)} records")
                extract_and_insert_data(conn, weather_data)
            except Exception as e:
                print(f"Error processing message as Parquet: {e}")
                
                with open('error_message.log', 'ab') as f:
                    f.write(message.value)
    
    print("Closing connection...")
    conn.close()

if __name__ == "__main__":
    main()
