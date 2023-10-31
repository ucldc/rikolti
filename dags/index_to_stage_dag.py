from datetime import datetime

from airflow.decorators import dag
from airflow.models.param import Param

from rikolti.dags.shared_tasks import cleanup_failed_index_creation_task
from rikolti.dags.shared_tasks import create_stage_index_task
from rikolti.dags.shared_tasks import get_collection_metadata_task
from rikolti.dags.shared_tasks import get_index_name_task

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
    # Once we start keeping dated versions of mapped metadata on S3,
    # the version will correspond to the S3 namespace
    datetime_string = datetime.today().strftime("%Y%m%d%H%M%S")
    index_name = get_index_name_task(collection=collection, version=datetime_string)
    create_stage_index_task(collection=collection, index_name=index_name) >> \
        cleanup_failed_index_creation_task(index_name=index_name)

index_collection_to_stage_dag()