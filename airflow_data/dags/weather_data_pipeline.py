from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'weather_data_pipeline_1',
    default_args=default_args,
    description='A DAG to run weather data pipeline scripts',
    schedule_interval=timedelta(days=1),
)

# Function to run the Python scripts
def run_script(script_path):
    python_executable = '/usr/local/bin/python3'  
    script_path = os.path.abspath(script_path)
    command = f'{python_executable} {script_path}'
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with return code {result.returncode} and error: {result.stderr}")
    print(result.stdout)

# Function to run the Kafka consumer script and insert data into Postgres
def run_consumer_and_insert_to_postgres():
    # First, run the Kafka consumer script
    consumer_script_path = os.path.abspath('/opt/airflow/scripts/4_kafka_consumer_to_postgres.py')
    command = f'/usr/local/bin/python3 {consumer_script_path}'
    result = subprocess.run(command, capture_output=True, text=True, shell=True)
    if result.returncode != 0:
        raise Exception(f"Script {consumer_script_path} failed with return code {result.returncode} and error: {result.stderr}")
    print(result.stdout)
    
    # Now, connect to Postgres and perform any additional operations if necessary
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    # Example SQL query (update this to your needs)
    insert_query = """
    INSERT INTO your_table (column1, column2)
    VALUES (%s, %s)
    """
    data_to_insert = ('value1', 'value2')
    cursor.execute(insert_query, data_to_insert)
    conn.commit()
    cursor.close()
    conn.close()

# Define tasks
t1 = PythonOperator(
    task_id='run_kafka_producer_from_weather_api',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/1_kafka_producer_from_weather_api.py'],  
    dag=dag,
)

t2 = PythonOperator(
    task_id='run_kafka_consumer_to_minio',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/2_kafka_consumer_to_minio.py'], 
    dag=dag,
)

t3 = PythonOperator(
    task_id='run_kafka_producer_from_minio',
    python_callable=run_script,
    op_args=['/opt/airflow/scripts/3_kafka_producer_from_minio.py'], 
    dag=dag,
)

t4 = PythonOperator(
    task_id='run_kafka_consumer_to_postgres',
    python_callable=run_consumer_and_insert_to_postgres,
    dag=dag,
)

# Set task dependencies
t1 >> t2 >> t3 >> t4
