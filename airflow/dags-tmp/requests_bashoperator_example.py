import sys
sys.path = [
    '/Users/awieliczka/Projects/rikolti/rikolti/'
] + sys.path[1:]

from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

import sys


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
def bash_operator_strat():
    fetch_collection_task = BashOperator(
        task_id='fetch_collection_task',
        bash_command=(
            'python '
            '/Users/awieliczka/Projects/rikolti/rikolti/metadata_fetcher/fetch_registry_collections.py '
            'https://registry.cdlib.org/api/v1/rikoltifetcher/26224/?format=json'
        ),
    )
    fetch_collection_task >> map_collection_task() >> validate_collection_task()

bash_operator_strat()
