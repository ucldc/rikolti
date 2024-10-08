import math
from datetime import datetime
from typing import Optional

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.dags.shared_tasks.mapping_tasks import mapping_tasks
from rikolti.dags.shared_tasks.mapping_tasks import validate_collection_task
from rikolti.dags.shared_tasks.shared import get_registry_data_task
from rikolti.dags.shared_tasks.shared import batched
from rikolti.utils.versions import get_most_recent_vernacular_version
from rikolti.utils.versions import get_most_recent_mapped_version
from rikolti.utils.versions import get_versioned_pages


@task(task_id="get_vernacular_page_batches")
def get_vernacular_page_batches_task(
    collection: dict, params: Optional[dict]=None) -> list[list[str]]:
    collection_id = collection['id']
    vernacular_version = params.get('vernacular_version') if params else None
    if not vernacular_version:
        vernacular_version = get_most_recent_vernacular_version(collection_id)
    pages = get_versioned_pages(vernacular_version)
    # TODO: split page_list into pages and children?

    # 1024 is the maximum number of fanout tasks allowed
    # so number of batches should never be more than 1024
    batch_size = math.ceil(len(pages) / 50)
    return batched(pages, batch_size)

@task(task_id="get_mapped_pages")
def get_mapped_pages_task(params: Optional[dict] = None):
    collection_id = params.get('collection_id') if params else None
    if not collection_id:
        raise Exception("Collection ID is required")
    mapped_version = params.get('mapped_version') if params else None
    if not mapped_version:
        mapped_version = get_most_recent_mapped_version(collection_id)
    pages = get_versioned_pages(mapped_version)
    return pages

# This is a functional duplicate of 
# rikolti.metadata_mapper.lambda_shepherd.map_collection

# Within an airflow runtime context, we take advantage of airflow's dynamic
# task mapping to fan out all calls to map_page. 
# Outside the airflow runtime context, on the command line for example, 
# map_collection performs manual "fan out" in the for loop below. 

# TODO: Any changes to mapper_dag should be carefully considered, duplicated
# to map_collection, and tested in both contexts. Resolve multiple contexts.

@dag(
    dag_id="map_collection",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to map"),
        'vernacular_version': Param(None, description="Vernacular version to map, ex: 3433/vernacular_metadata_v1/")
    },
    tags=["rikolti"],
)
def mapper_dag():
    collection = get_registry_data_task()
    page_batches = get_vernacular_page_batches_task(collection=collection)
    mapping_tasks(collection, page_batches)
mapper_dag()

@dag(
    dag_id="validate_collection",
    schedule=None,
    start_date=datetime(2023,1,1),
    catchup=False,
    params={
        'collection_id': Param(None, description="Collection ID to validate"),
        "vernacular_version": Param(None, description="Vernacular version to validate")
    },
    tags=["rikolti"]
)
def validation_dag():
    collection = get_registry_data_task()
    page_list = get_mapped_pages_task()
    validate_collection_task(collection['id'], page_list)

validation_dag()