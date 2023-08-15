from datetime import datetime

import requests

from airflow.decorators import dag, task
from airflow.models.param import Param

from metadata_fetcher.lambda_function import fetch_collection


@task()
def fetch_collection_task(dag_run=None):
    if not dag_run:
        return False

    collection_id = dag_run.conf.get('collection_id')
    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikoltifetcher/{collection_id}/"
    )
    resp.raise_for_status()    
    fetch_report = fetch_collection(resp.json(), {})

    return True


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(1, description="Collection ID to fetch")},
    tags=["rikolti"],
)
def fetcher_dag():
    fetch_collection_task()

fetcher_dag()
