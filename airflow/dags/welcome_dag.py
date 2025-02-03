from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow!")

def print_welcome():
    print("Welcome to your first DAG!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('welcome_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    task2 = PythonOperator(
        task_id='print_welcome',
        python_callable=print_welcome
    )

    task1 >> task2