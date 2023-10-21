from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.dags.shared_tasks import create_new_index_task
from rikolti.dags.shared_tasks import get_collection_metadata_task

@dag(
    dag_id="index_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to fetch")},
    tags=["rikolti"],
)
def indexer_dag():
    collection = get_collection_metadata_task()
    create_new_index_task(collection=collection)

indexer_dag()