from datetime import datetime

import requests
from airflow.decorators import task
from urllib.parse import urlparse

from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv


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
    fetch_status = fetch_collection(collection, {})

    success = all([page['status'] == 'success' for page in fetch_status])
    total_items = sum([page['document_count'] for page in fetch_status])
    total_pages = fetch_status[-1]['page'] + 1
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
        str(page['page']) for page in fetch_status if page['status']=='success'
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
def get_mapping_status_task(collection: dict, mapped_pages: list):
    mapping_status = get_mapping_status(collection, mapped_pages)
    return mapping_status

@task()
def validate_collection_task(collection_status: dict, params=None) -> str:
    if not params or not params.get('validate'):
        raise ValueError("Validate flag not found in params")

    # let this throw an error if no collection_id
    collection_id = int(collection_status['collection_id'])

    if collection_status.get('status') != 'success':
        raise Exception(f"Collection {collection_id} not successfully mapped")

    num_rows, file_location = create_collection_validation_csv(collection_id)
    print(f"Output {num_rows} rows to {file_location}")

    # create a link to the file in the logs
    if file_location.startswith('s3://'):
        parsed_loc = urlparse(file_location)
        file_location = (
            f"https://{parsed_loc.netloc}.s3.us-west-2."
            f"amazonaws.com{parsed_loc.path}"
        )

    return file_location