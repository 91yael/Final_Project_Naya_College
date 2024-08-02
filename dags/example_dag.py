from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define default arguments
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
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the tasks
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

def my_python_task():
    print("Hello from Python task!")

t2 = PythonOperator(
    task_id='python_task',
    python_callable=my_python_task,
    dag=dag,
)

# Set the task dependencies
t1 >> t2
