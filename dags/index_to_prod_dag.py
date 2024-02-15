from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.dags.shared_tasks.indexing_tasks import move_index_to_prod_task

@dag(
    dag_id="index_collection_to_prod",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to move to prod")},
    tags=["rikolti"],
)
def index_to_prod_dag():
    collection = get_registry_data_task()
    move_index_to_prod_task(collection=collection)

index_to_prod_dag()