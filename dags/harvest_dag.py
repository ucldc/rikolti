import requests

from datetime import datetime

from airflow.decorators import dag, task_group, task
from airflow.models.param import Param

from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_mapper.lambda_shepherd import get_vernacular_pages
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import \
    get_mapping_summary, check_for_missing_enrichments


@task()
def fetch_collection_task(params=None):
    if not params or not params.get('collection_id'):
        return False
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikoltifetcher/{collection_id}/?format=json"
    )
    resp.raise_for_status()

    fetch_report = fetch_collection(resp.json(), {})

    return fetch_report


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
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    pages = get_vernacular_pages(collection_id)
    return pages


# max_active_tis_per_dag - setting on the task to restrict how many
# instances can be running at the same time, *across all DAG runs*
@task()
def map_page_task(page: str, collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        return False
    mapped_page = map_page(collection_id, page, collection)
    return mapped_page


@task()
def get_mapping_summary_task(mapped_pages: list, collection: dict):
    collection_id = collection.get('id')
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


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    params={'collection_id': Param(None, description="Collection ID to map")},
    tags=["rikolti"],
)
def harvest():
    collection = get_collection_metadata_task()

    @task_group(group_id="fetching")
    def fetching(collection):
        fetch_report = fetch_collection_task()

    @task_group(group_id="mapping")
    def mapping(collection):
        page_list = get_vernacular_pages_task(collection=collection)
        mapped_pages = (
            map_page_task
                .partial(collection=collection)
                .expand(page=page_list)
        )
        get_mapping_summary_task(mapped_pages, collection)
    
    fetching(collection) >> mapping(collection)

harvest()