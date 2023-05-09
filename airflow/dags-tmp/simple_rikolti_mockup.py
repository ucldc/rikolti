from airflow.decorators import task, dag
from datetime import datetime, timedelta

import xmltodict
import sys

@task
def fetch_collection_task():
    # can I even use third party libraries? where are airflow executors running?
    print(xmltodict.parse('<a>1</a>'))
    print(sys.path)
    print('fetching')
    return True


@task
def map_collection_task():
    print("mapping")
    return True


@task
def validate_collection_task():
    print("validating")
    return True


@dag(
    default_args={
        'start_date': datetime(2023, 5, 8),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=None,
    catchup=False,
    tags=['rikolti'],
)
def simple_rikolti_mockup():
    fetch_collection_task >> map_collection_task() >> validate_collection_task()
    
simple_rikolti_mockup()
