import sys
sys.path = [
    '/Users/awieliczka/Projects/rikolti/rikolti/'
] + sys.path[1:]

from airflow.decorators import task, dag
from datetime import datetime, timedelta
from metadata_fetcher.fetch_registry_collections import fetch_endpoint

import sys


@task
def fetch_collection():
    print("fetching")
    return fetch_endpoint(
        'https://registry.cdlib.org/api/v1/rikoltifetcher/26224/?format=json'
    )


@task
def map_collection():
    print("mapping")
    return True
    # return map_endpoint(
    #     'https://registry.cdlib.org/api/v1/rikoltifetcher/26224/?format=json'
    # )


@task
def validate_collection():
    print("validating")
    return True
    # return create_collection_validation_csv(26224)


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
def harvest_collection():
    fetch_collection >> map_collection() >> validate_collection()

harvest_collection()
