from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


def push_data(**context):
    context['ti'].xcom_push(key='my_key', value='my_value')


def pull_data(**context):
    value = context['ti'].xcom_pull(key='my_key')
    print(value)


def test_push_data():
    dag = DAG(dag_id='test_push_data', start_date=days_ago(1))
    task = PythonOperator(task_id='push_data', python_callable=push_data, dag=dag)
    task.run(start_date=days_ago(1), end_date=days_ago(1))


def test_pull_data():
    dag = DAG(dag_id='test_pull_data', start_date=days_ago(1))
    task = PythonOperator(task_id='pull_data', python_callable=pull_data, dag=dag)
    task.run(start_date=days_ago(1), end_date=days_ago(1))


def test_xcom_dag():
    test_push_data()
    test_pull_data()


test_xcom_dag()