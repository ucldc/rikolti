from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.shared_tasks import get_registry_data_task
from rikolti.dags.shared_tasks.fetching_tasks import fetching_tasks

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
    fetching_tasks(collection)

fetcher_dag()
