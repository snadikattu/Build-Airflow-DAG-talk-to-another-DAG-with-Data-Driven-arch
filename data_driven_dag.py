# This DAG demonstrates how to use the XCom feature in Airflow to enable dynamic data exchange between two DAGs.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.xcom import XCom
from datetime import datetime, timedelta


# Define the push_xcom function to push data to XCom

def push_xcom(**context):
    ti = context['ti']
    ti.xcom_push(key='my_xcom_key', value='my_xcom_value')


# Define the pull_xcom function to pull data from XCom

def pull_xcom(**context):
    ti = context['ti']
    xcom_value = ti.xcom_pull(key='my_xcom_key', task_ids='push_xcom')
    print(xcom_value)


# Define the DAG
with DAG('data_driven_dag', start_date=datetime(2023, 5, 13), schedule_interval='@daily') as dag:
    # Define the tasks
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    push_xcom = PythonOperator(task_id='push_xcom', python_callable=push_xcom, provide_context=True)
    pull_xcom = PythonOperator(task_id='pull_xcom', python_callable=pull_xcom, provide_context=True)

    # Define the task dependencies
    start >> push_xcom >> pull_xcom >> end
