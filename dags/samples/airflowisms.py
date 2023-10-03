import os

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python import get_current_context

import requests

@task()
def taskflow_test_requests():
    """ this did not work with airflow standalone """
    resp = requests.get("https://google.com")
    resp.raise_for_status()
    return resp.status_code

@task()
def taskflow_params(dag_run=None, params=None):
    """ three ways to get dag run parameters """

    if dag_run:
        print(dag_run.conf.get('collection_id'))

    if params:
        print(params.get('collection_id'))

    context = get_current_context()
    collection_id = context.get('params', {}).get('collection_id')
    print(collection_id)

    return collection_id

@task()
def taskflow_get_admin_variables():
    """ get admin variables from airflow db """
    airflow_test_env = Variable.get("AIRFLOW_TEST")
    print(airflow_test_env)
    os.environ["AIRFLOW_TEST"] = airflow_test_env

    boo = os.environ.get("AIRFLOW_TEST")
    print(boo)

    return True

@task()
def taskflow_get_env():
    """ get env variables previously set """
    startup_env = os.environ.get("ENVIRONMENT_STAGE")
    print(startup_env)
    return startup_env

@task()
def taskflow_print(message=None):
    print(message)
    return message

@task()
def fails_sometimes():
    """ sometimes this task fails """
    nowish = datetime.now().second
    print(nowish)
    print(nowish % 2)
    if nowish % 2 == 0:
        raise Exception(f"this task fails sometimes: {nowish} % 2 == 0")
    return True

@task()
def downstream_should_fail(upstream_result):
    """ this task should never run """
    print(upstream_result)
    return True

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(1, description="Collection ID")
    },
    tags=["sample"],
)
def sample_airflowisms():
    dag_var = "boo"
    if os.environ.get("ENVIRONMENT_STAGE") == "dev":
        dag_var = "foo"

    taskflow_print(dag_var)
    taskflow_print(os.environ.get("ENVIRONMENT_STAGE"))
    taskflow_test_requests()
    taskflow_params()
    taskflow_get_admin_variables()
    taskflow_get_env()
    result = fails_sometimes()
    downstream_should_fail(result)

sample_airflowisms()
