import requests

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from rikolti.metadata_mapper.lambda_shepherd import (get_vernacular_pages,
    get_mapping_summary, check_for_missing_enrichments)
from rikolti.metadata_mapper.lambda_function import map_page


@task()
def get_collection_metadata_task(params=None):
    if not params or not params.get('collection_id'):
        return False
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikolticollection/{collection_id}/?format=json"
    )
    resp.raise_for_status()

    return resp.json()


@task()
def get_vernacular_pages_task(collection: dict):
    collection_id = collection.get('collection_id')
    if not collection_id:
        return False
    pages = get_vernacular_pages(collection_id)
    return pages


# max_active_tis_per_dag - setting on the task to restrict how many
# instances can be running at the same time, *across all DAG runs*
@task()
def map_page_task(page: str, collection: dict):
    collection_id = collection.get('collection_id')
    if not collection_id:
        return False
    mapped_page = map_page(collection_id, page, collection)
    return mapped_page


@task()
def get_mapping_summary_task(mapped_pages: list, collection: dict):
    collection_id = collection.get('collection_id')
    collection_summary = get_mapping_summary(mapped_pages)

    return {
        'status': 'success',
        'collection_id': collection_id,
        'pre_mapping': collection.get('rikolti__pre_mapping'),
        'enrichments': collection.get('rikolti__enrichments'),
        'missing_enrichments': check_for_missing_enrichments(collection),
        'records_mapped': collection_summary.get('count'),
        'pages_mapped': collection_summary.get('page_count'),
        'exceptions': collection_summary.get('group_exceptions')
    }


# This is a functional duplicate of 
# rikolti.metadata_mapper.lambda_shepherd.map_collection

# Within an airflow runtime context, we take advantage of airflow's dynamic
# task mapping to fan out all calls to map_page. 
# Outside the airflow runtime context, on the command line for example, 
# map_collection performs manual "fan out" in the for loop below. 

# TODO: Any changes to mapper_dag should be carefully considered, duplicated
# to map_collection, and tested in both contexts. Resolve multiple contexts.

# TODO: this is a simple dynamic task mapping w/ max_map_length=1024 by default
# if get_vernacular_pages_task() returns more than 1024 pages, map_page_task
# will fail - need to somehow chunk up pages into groups of 1024?

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to map")},
    tags=["rikolti"],
)
def mapper_dag():
    collection = get_collection_metadata_task()
    page_list = get_vernacular_pages_task(collection=collection)
    mapped_pages = (
        map_page_task
            .partial(collection=collection)
            .expand(page=page_list)
    )

    get_mapping_summary_task(mapped_pages, collection)
mapper_dag()
