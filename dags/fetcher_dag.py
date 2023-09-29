from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param


from rikolti.dags.harvest_dag import fetch_collection_task

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
