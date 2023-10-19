from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param


from rikolti.dags.shared_tasks import fetch_collection_task
from rikolti.dags.shared_tasks import get_collection_fetchdata_task
from rikolti.dags.shared_tasks import get_collection_metadata_task
from rikolti.dags.shared_tasks  import map_page_task
from rikolti.dags.shared_tasks  import get_mapping_status_task
from rikolti.dags.shared_tasks import validate_collection_task
from rikolti.dags.shared_tasks import ContentHarvestDockerOperator

@task()
def get_mapped_page_filenames_task(mapped_pages):
    return [mapped['page_filename'] for mapped in mapped_pages]

@dag(
    dag_id="harvest_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to harvest"),
        'validate': Param(True, description="Validate mapping?")
    },
    tags=["rikolti"],
)
def harvest():

    # see TODO at get_collection_fetchdata_task
    fetchdata = get_collection_fetchdata_task()
    collection = get_collection_metadata_task()

    fetched_pages = fetch_collection_task(collection=fetchdata)
    mapped_pages = (
        map_page_task
            .partial(collection=collection)
            .expand(page=fetched_pages)
    )

    mapping_status = get_mapping_status_task(collection, mapped_pages)
    validate_collection_task(mapping_status)
    mapped_page_filenames = get_mapped_page_filenames_task(mapped_pages)

    content_harvest_task = (
        ContentHarvestDockerOperator
            .partial(
                task_id="content_harvest", 
                collection_id="{{ params.collection_id }}",
            )
            .expand(
                page=mapped_page_filenames
            )
    )
    content_harvest_task
    

harvest()