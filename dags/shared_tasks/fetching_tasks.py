import math
import pprint
from datetime import datetime
from typing import Optional

from airflow.decorators import task, task_group

from rikolti.dags.shared_tasks.shared_tasks import batched
from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.utils.versions import create_vernacular_version


@task()
def create_vernacular_version_task(collection) -> str:
    # returns: '3433/vernacular_metadata_v1/'
    return create_vernacular_version(collection.get('collection_id'))


@task()
def fetch_collection_task(
    collection: dict, vernacular_version: str) -> list[list[str]]:
    """
    returns a list of the filepaths of the vernacular metadata relative to the
    collection id, ex: [
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/1',
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/2'
    ]
    """
    def flatten_stats(stats):
        success = all([page_stat['status'] == 'success' for page_stat in stats])
        total_items = sum([page_stat['document_count'] for page_stat in stats])
        total_pages = len(stats)
        filepaths = [page_stat['vernacular_filepath'] for page_stat in stats]

        children = False
        for page_stat in stats:
            child_pages = page_stat.get('children')
            if child_pages:
                flat_stats = flatten_stats(child_pages)
                success = success and flat_stats['success']
                total_items = total_items + flat_stats['total_items']
                total_pages = total_pages + flat_stats['total_pages']
                filepaths = filepaths + flat_stats['filepaths']
                children = True

        return {
            'success': success,
            'total_items': total_items,
            'total_pages': total_pages,
            'filepaths': filepaths,
            'children': children
        }

    fetch_status = fetch_collection(collection, vernacular_version)
    stats = flatten_stats(fetch_status)
    total_parent_items = sum([page['document_count'] for page in fetch_status])
    diff_items = total_parent_items - collection['solr_count']
    date = None
    if collection.get('solr_last_updated'):
        date = datetime.strptime(
            collection['solr_last_updated'],
            "%Y-%m-%dT%H:%M:%S.%f"
        )

    print(
        f"{'Successfully fetched' if stats['success'] else 'Error fetching'} "
        f"collection {collection['collection_id']}"
    )
    print(
        f"Fetched {stats['total_items']} items across {stats['total_pages']} "
        f"pages at a rate of ~{stats['total_items'] / stats['total_pages']} "
        "items per page"
    )
    if stats['children']:
        print(
            f"Fetched {total_parent_items} parent items across "
            f"{len(fetch_status)} pages and "
            f"{stats['total_items']-total_parent_items} child items across "
            f"{stats['total_pages']-len(fetch_status)} child pages"
        )
    if date:
        print(
            f"As of {datetime.strftime(date, '%B %d, %Y %H:%M:%S.%f')} "
            f"Solr has {collection['solr_count']} items"
        )
    else:
        print("The collection registry has no record of Solr count.")
    if diff_items != 0:
        print(
            f"Rikolti fetched {abs(diff_items)} "
            f"{'more' if diff_items > 0 else 'fewer'} items."
        )

    if not stats['filepaths'] or not stats['success']:
        raise Exception(
            f"vernacular metadata not successfully fetched"
            f"\n{pprint.pprint(fetch_status)}\n{stats['success']}\n{stats['filepaths']}"
        )

    # 1024 is the maximum number of fanout tasks allowed
    # so number of batches should never be more than 1024
    batch_size = math.ceil(len(stats['filepaths']) / 1024)
    return batched(stats['filepaths'], batch_size)


@task_group(group_id='fetching')
def fetching_tasks(collection: Optional[dict] = None):
    vernacular_version = create_vernacular_version_task(
        collection=collection['registry_fetchdata'])
    fetched_page_batches = fetch_collection_task(
        collection=collection['registry_fetchdata'], 
        vernacular_version=vernacular_version)
    return fetched_page_batches


