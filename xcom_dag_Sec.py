# Importing the required libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Defining the DAG
with DAG('xcom_dag', start_date=datetime(2023, 5, 13), schedule_interval='@once') as dag:
    
    # Defining the first task
    def push_data(**context):
        context['ti'].xcom_push(key='data', value='Hello World!')
        print('Data pushed to XCom')
    
    # Defining the second task
    def pull_data(**context):
        data = context['ti'].xcom_pull(key='data')
        print(f'Data pulled from XCom: {data}')
    
    # Creating the first task
    push_task = PythonOperator(
        task_id='push_data',
        python_callable=push_data,
        provide_context=True
    )
    
    # Creating the second task
    pull_task = PythonOperator(
        task_id='pull_data',
        python_callable=pull_data,
        provide_context=True
    )
    
    # Setting the task dependencies
    push_task >> pull_task
