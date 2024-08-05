import os

# Set the AIRFLOW_HOME environment variable to an absolute path
os.environ['AIRFLOW_HOME'] = 'C:/Users/97254/airflow'

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time
from airflow.configuration import conf

# Set the sql_alchemy_conn parameter to use an absolute path for SQLite
conf.set('core', 'sql_alchemy_conn', 'sqlite:////C:/Users/97254/airflow/airflow.db')

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define a Python function that will be used in a task
def print_date():
    print(f"Current date is: {datetime.now()}")

def print_hello():
    print("Hello World")

# Define the tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag,
)

sleep_task = PythonOperator(
    task_id='sleep',
    python_callable=lambda: time.sleep(5),
    dag=dag,
)

print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Define the task dependencies
start >> print_date_task >> sleep_task >> print_hello_task >> end
