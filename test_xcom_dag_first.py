import pytest
from airflow.models import DagBag
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.xcom import XCom


def test_dag():
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='test_xcom_dag')
    assert dag is not None


def test_task():
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='test_xcom_dag')
    task = dag.get_task(task_id='push_xcom')
    assert task is not None


def test_xcom():
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id='test_xcom_dag')
    push_task = dag.get_task(task_id='push_xcom')
    pull_task = dag.get_task(task_id='pull_xcom')
    now = timezone.utcnow()
    push_task.execute(context={'execution_date': now})
    pull_task.execute(context={'execution_date': now})
    xcom_value = XCom.get(key='my_xcom_key', dag_id='test_xcom_dag', execution_date=now)
    assert xcom_value == 'my_xcom_value'
