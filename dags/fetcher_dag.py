from datetime import datetime

import requests

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.metadata_fetcher.lambda_function import fetch_collection


@task()
def fetch_collection_task(params=None):
    if not params or not params.get('collection_id'):
        return False
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikoltifetcher/{collection_id}/?format=json"
    )
    resp.raise_for_status()

    fetch_report = fetch_collection(resp.json(), {})

    return fetch_report


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to fetch")},
    tags=["rikolti"],
)
def fetcher_dag():
    fetch_collection_task()

fetcher_dag()
