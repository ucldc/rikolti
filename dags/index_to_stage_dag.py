from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks import cleanup_failed_index_creation_task
from rikolti.dags.shared_tasks import create_stage_index_task
from rikolti.dags.shared_tasks import get_collection_metadata_task

@dag(
    dag_id="index_collection_to_stage",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to index")},
    tags=["rikolti"],
)
def index_collection_to_stage_dag():
    collection = get_collection_metadata_task()
    index_name = create_stage_index_task(collection=collection)
    cleanup_failed_index_creation_task(index_name=index_name)

index_collection_to_stage_dag()