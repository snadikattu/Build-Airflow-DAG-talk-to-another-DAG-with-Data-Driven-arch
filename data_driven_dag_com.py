# Import the necessary modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'data_driven_dag',
    default_args=default_args,
    description='A data-driven DAG',
    schedule_interval=timedelta(days=1),
) as dag:
    
    # Define the tasks
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Task 1"',
    )
    
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Task 2"',
    )
    
    task3 = PythonOperator(
        task_id='task3',
        python_callable=print_xcom,
    )
    
    task4 = DummyOperator(
        task_id='task4',
    )
    
    # Define the dependencies
    task1 >> task2 >> task3 >> task4
    
    # Define the XComs
    task1_xcom = {
        'task_id': 'task1',
        'key': 'task1_data',
        'value': 'Task 1 Data',
    }
    
    task2_xcom = {
        'task_id': 'task2',
        'key': 'task2_data',
        'value': 'Task 2 Data',
    }
    
    # Define the XCom pushers
    task1_push = XComPusher(
        task_id='task1_push',
        xcoms=[task1_xcom],
    )
    
    task2_push = XComPusher(
        task_id='task2_push',
        xcoms=[task2_xcom],
    )
    
    # Define the XCom pullers
    task3_pull = XComPuller(
        task_id='task3_pull',
        xcom_keys=['task1_data', 'task2_data'],
        python_callable=process_xcom,
    )
    
    # Define the DAG dependencies
    task1 >> task1_push >> task3_pull
    task2 >> task2_push >> task3_pull
    task3_pull >> task4


# Define the function to print the XCom
# This function will be called by task3
# It will print the XCom data that it receives
# from task1 and task2
def print_xcom(**context):
    task1_data = context['task_instance'].xcom_pull(task_ids='task1', key='task1_data')
    task2_data = context['task_instance'].xcom_pull(task_ids='task2', key