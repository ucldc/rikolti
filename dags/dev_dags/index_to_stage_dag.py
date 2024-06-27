from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks.indexing_tasks import (
    stage_collection_task, get_version_pages)
from rikolti.dags.shared_tasks.shared import get_registry_data_task


@dag(
    dag_id="index_collection_to_stage",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to index"),
        'version': Param(None, description="Version path to index")
    },
    tags=["rikolti"],
)
def index_collection_to_stage_dag():
    collection = get_registry_data_task()
    version_pages = get_version_pages()
    index_name = stage_collection_task(collection, version_pages)  # noqa F841

index_collection_to_stage_dag()