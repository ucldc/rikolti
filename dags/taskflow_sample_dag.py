import os

from datetime import datetime
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python import get_current_context

# from airflow.operators.python import PythonOperator
import requests

@task()
def taskflow_test_requests():
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
def taskflow_mkdir():
    """ we have permissions inside /airflow/, but nowhere else it seems """
    if os.path.exists("/usr/local/airflow/rikolti_data/test_dir"):
        os.remove("/usr/local/airflow/rikolti_data/test_dir/test2.txt")
        os.rmdir("/usr/local/airflow/rikolti_data/test_dir")

    os.mkdir("/usr/local/airflow/rikolti_data/test_dir")
    with open("/usr/local/airflow/rikolti_data/test_dir/test2.txt", "w") as f:
        f.write("hi amy")
    return True

@task()
def taskflow_write_to_disk():
    """ write a file to disk """
    with open("/usr/local/airflow/rikolti_data/test.txt", "w") as f:
        f.write("hello world")
    return True

@task()
def taskflow_get_env():
    """ get env variables previously set """
    startup_env = os.environ.get("ENVIRONMENT_STAGE")
    print(startup_env)
    return startup_env

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
    tags=["test"],
)
def taskflow_test_dag():
    taskflow_test_requests()
    taskflow_params()
    taskflow_get_admin_variables()
    taskflow_mkdir()
    taskflow_write_to_disk()
    taskflow_get_env()
    result = fails_sometimes()
    downstream_should_fail(result)

taskflow_test_dag()
