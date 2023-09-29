from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks import get_collection_fetchdata_task
from rikolti.dags.shared_tasks import fetch_collection_task

@dag(
    dag_id="fetch_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to fetch")},
    tags=["rikolti"],
)
def fetcher_dag():
    fetchdata = get_collection_fetchdata_task()
    fetch_collection_task(collection=fetchdata)

fetcher_dag()
