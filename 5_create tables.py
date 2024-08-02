import os
import psycopg2
from dotenv import load_dotenv

def create_tables():
    commands = [
        """
        CREATE TABLE weather_data (
            id SERIAL PRIMARY KEY,
            city_name VARCHAR(100),
            country_code VARCHAR(10),
            latitude FLOAT,
            longitude FLOAT,
            state_code VARCHAR(10),
            timezone VARCHAR(50)
        )
        """,
        """
        CREATE TABLE forecast_data (
            id SERIAL PRIMARY KEY,
            weather_data_id INTEGER REFERENCES weather_data(id) ON DELETE CASCADE,
            date DATE,
            high_temp FLOAT,
            low_temp FLOAT,
            average_temp FLOAT,
            precipitation FLOAT,
            humidity INTEGER,
            wind_speed FLOAT,
            wind_direction VARCHAR(10),
            gust_speed FLOAT,
            weather_description VARCHAR(255),
            weather_icon VARCHAR(10),
            uv_index INTEGER,
            visibility FLOAT
        )
        """
    ]
    
    try:
        # Load environment variables from .env file
        load_dotenv()

        # Get database connection details from environment variables
        postgres_host = os.getenv('POSTGRES_HOST')
        postgres_port = os.getenv('POSTGRES_PORT', 5432)  # Default to 5432 if not set
        postgres_db = os.getenv('POSTGRES_DB')
        postgres_user = os.getenv('POSTGRES_USER')
        postgres_password = os.getenv('POSTGRES_PASSWORD')

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )
        cursor = conn.cursor()

        # Execute each command to create the tables
        for command in commands:
            cursor.execute(command)

        # Commit the changes
        conn.commit()

        # Close communication with the PostgreSQL database
        cursor.close()
        conn.close()

        print("Tables created successfully.")
    except Exception as err:
        print(f"An error occurred while creating tables: {err}")

if __name__ == "__main__":
    create_tables()
