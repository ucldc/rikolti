import requests
from datetime import datetime

from airflow.decorators import dag, task_group, task
from airflow.models.param import Param

from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import \
    get_mapping_summary, check_for_missing_enrichments


# TODO: remove the rikoltifetcher registry endpoint and restructure
# the fetch_collection function to accept a rikolticollection resource.
@task()
def get_collection_fetchdata_task(params=None):
    if not params or not params.get('collection_id'):
        raise ValueError("Collection ID not found in params")
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikoltifetcher/{collection_id}/?format=json"
    )
    resp.raise_for_status()

    return resp.json()


@task()
def fetch_collection_task(collection: dict):
    fetch_report = fetch_collection(collection, {})

    success = all([page['status'] == 'success' for page in fetch_report])
    total_items = sum([page['document_count'] for page in fetch_report])
    total_pages = fetch_report[-1]['page'] + 1
    diff_items = total_items - collection['solr_count']
    date = datetime.strptime(
        collection['solr_last_updated'],
        "%Y-%m-%dT%H:%M:%S.%f"
    )

    print(
        f"{'Successfully fetched' if success else 'Error fetching'} "
        f"collection {collection['collection_id']}"
    )
    print(
        f"Fetched {total_items} items across {total_pages} pages "
        f"at a rate of ~{total_items / total_pages} items per page"
    )
    print(
        f"As of {datetime.strftime(date, '%B %d, %Y %H:%M:%S.%f')} "
        f"Solr has {collection['solr_count']} items"
    )
    if diff_items != 0:
        print(
            f"Rikolti fetched {abs(diff_items)} "
            f"{'more' if diff_items > 0 else 'fewer'} items."
        )

    return [
        str(page['page']) for page in fetch_report if page['status'] == 'success'
    ]


@task()
def get_collection_metadata_task(params=None):
    if not params or not params.get('collection_id'):
        raise ValueError("Collection ID not found in params")
    collection_id = params.get('collection_id')

    resp = requests.get(
        "https://registry.cdlib.org/api/v1/"
        f"rikolticollection/{collection_id}/?format=json"
    )
    resp.raise_for_status()

    return resp.json()


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
    params={'collection_id': Param(3433, description="Collection ID to map")},
    tags=["rikolti"],
)
def harvest():

    fetchdata = get_collection_fetchdata_task()
    collection = get_collection_metadata_task()

    fetched_pages = fetch_collection_task(collection=fetchdata)
    mapped_pages = (
        map_page_task
            .partial(collection=collection)
            .expand(page=fetched_pages)
    )
    get_mapping_summary_task(mapped_pages, collection)
    

harvest()