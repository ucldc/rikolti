from datetime import datetime
import json
import sys

from airflow.decorators import dag, task
from airflow.models.param import Param

from rikolti.metadata_mapper.lambda_shepherd import get_vernacular_pages, get_collection
from rikolti.metadata_mapper.lambda_function import map_page


@task()
def get_vernacular_pages_for_collection_task(params=None):
    if not params:
        return False

    collection_id = params.get('collection_id')
    # raise an error?
    if not collection_id:
        return []

    pages = get_vernacular_pages(
                collection_id)

    return pages

@task()
def get_registry_metadata_for_collection_task(params=None):
    if not params:
        return False

    collection_id = params.get('collection_id')
    # raise an error?
    if not collection_id:
        return []

    collection = get_collection(collection_id)

    return collection


@task()
def map_page_task(page: str, collection: dict, params=None):
    if not params:
        return False

    collection_id = params.get('collection_id')
    # raise an error?
    if not collection_id:
        return {}

    try:
        mapped_page = map_page(collection_id, page, collection)
    except KeyError:
        print(
            f"[{collection_id}]: {collection['rikolti_mapper_type']} "
            "not yet implemented", file=sys.stderr
        )

    return mapped_page

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to map")},
    tags=["rikolti"],
)
def mapper_dag():
    collection = get_registry_metadata_for_collection_task()

    # simple dynamic task mapping
    # max_map_length=1024 by default. 
    # if get_vernacular_pages_for_collection_task() generates more than this, that task will fail
    # need to somehow chunk up pages into groups of 1024?
    map_page_task.partial(collection=collection).expand(page=get_vernacular_pages_for_collection_task())
    
    # max_active_tis_per_dag - setting on the task to restrict how many
    # instances can be running at the same time, *across all DAG runs*

mapper_dag()
