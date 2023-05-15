import pytest
from airflow.models import DagBag


def test_push_to_xcom():
    dagbag = DagBag()
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag(dag_id='xcom_dag')
    task = dag.get_task(task_id='push_to_xcom')
    result = task.execute(None)
    assert result == 'Hello, world!'
    assert task.xcom_pull(key='test_key') == 'Hello, world!'


def test_pull_from_xcom():
    dagbag = DagBag()
    assert dagbag.import_errors == {}
    dag = dagbag.get_dag(dag_id='xcom_dag')
    task = dag.get_task(task_id='pull_from_xcom')
    result = task.execute(None)
    assert result == 'Hello, world!'
    assert task.xcom_pull(key='test_key') == 'Hello, world!'
