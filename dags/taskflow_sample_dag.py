from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

# from airflow.operators.python import PythonOperator
import requests

@task()
def taskflow_test_requests(dag_run=None, params=None):
    """ three ways to get dag run parameters """

    if dag_run:
        print(dag_run.conf.get('collection_id'))

    if params:
        print(params.get('collection_id'))

    context = get_current_context()
    collection_id = context.get('params', {}).get('collection_id')
    print(collection_id)

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikolticollection/{collection_id}/"
    )
    resp.raise_for_status()
    print(resp.json().get('name'))

    return True

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(1, description="Collection ID")
    },
    tags=["test"],
)
def taskflow_test_dag():
    taskflow_test_requests()

taskflow_test_dag()
