# Final_Project_Naya_College

## Flight Weather Correlation Project

### Overview
The Flight Weather Correlation Project aims to analyze and correlate flight data with weather conditions. This project processes flight data to filter for direct flights, extracts essential fields, and identifies the cheapest direct flights. Additionally, it integrates weather information to provide a comprehensive analysis of how weather impacts flight costs.

### Features
* Data collection from data sources: weather & flights
* Determines the cheapest direct flights based on cleaned data
* Kafka Messaging: Sends the data to a Kafka topics for further processing
* Database Management: Inserts or updates data in a PostgreSQL database

### Docker Setup
1. Build and Start Docker Containers using the Yaml script
2. Required Containers: Airflow, Kafka, PostgreSQL, Minio

### Installation
1. Clone the Repository: git clone https://github.com/91yael/Final_Project_Naya_College.git
2. Set Up the Environment: pip install -r requirements.txt
3. Database Configuration: Update your PostgreSQL configuration and the Minio authentication on .env file

### Run the Project
For running the project, please run the "dags" in the "airflow_data" folder
