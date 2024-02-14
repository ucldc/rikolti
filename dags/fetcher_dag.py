from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.shared_tasks import get_registry_data_task
from rikolti.dags.shared_tasks.shared_tasks import fetch_collection_task
from rikolti.dags.shared_tasks.shared_tasks import create_vernacular_version_task

@dag(
    dag_id="fetch_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to fetch")},
    tags=["rikolti"],
)
def fetcher_dag():
    collection = get_registry_data_task()
    vernacular_version = create_vernacular_version_task(
        collection=collection['registry_fetchdata'])
    fetch_collection_task(
        collection=collection['registry_fetchdata'], 
        vernacular_version=vernacular_version
    )

fetcher_dag()
