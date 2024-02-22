import logging
import math
import pprint
from datetime import datetime
from dataclasses import asdict
from typing import Optional

from airflow.decorators import task, task_group

from rikolti.dags.shared_tasks.shared import batched
from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_fetcher.fetch_registry_collections import fetch_endpoint
from rikolti.utils.versions import create_vernacular_version
from rikolti.utils.versions import get_version

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
    def flatten_stats(page_statuses):
        total_items = sum([ps.document_count for ps in page_statuses])
        total_pages = len(page_statuses)
        filepaths = [ps.vernacular_filepath for ps in page_statuses]

        children = False
        for page_status in page_statuses:
            child_pages = page_status.get('children')
            if child_pages:
                flat_stats = flatten_stats(child_pages)
                total_items = total_items + flat_stats['total_items']
                total_pages = total_pages + flat_stats['total_pages']
                filepaths = filepaths + flat_stats['filepaths']
                children = True

        return {
            'total_items': total_items,
            'total_pages': total_pages,
            'filepaths': filepaths,
            'children': children
        }

    page_statuses = fetch_collection(collection, vernacular_version)
    stats = flatten_stats(page_statuses)
    total_parent_items = sum([page.document_count for page in page_statuses])
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
            f"{len(page_statuses)} pages and "
            f"{stats['total_items']-total_parent_items} child items across "
            f"{stats['total_pages']-len(page_statuses)} child pages"
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
            f"\n{pprint.pprint([asdict(ps) for ps in page_statuses])}\n{stats['success']}\n{stats['filepaths']}"
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


logger = logging.getLogger("airflow.task")


@task()
def fetch_endpoint_task(endpoint, params=None):
    """
    3433: [
            {
                document_count: int
                vernacular_filepath: path relative to collection id
                    ex: "3433/vernacular_version_1/data/1"
                status: 'success' or 'error'
            }
        ]
    """
    limit = params.get('limit', None) if params else None
    fetcher_job_result = fetch_endpoint(endpoint, limit, logger)
    fetched_versions = {}
    errored_collections = {}
    for collection_id, page_statuses in fetcher_job_result.items():
        if not isinstance(page_statuses, list):
            errored_collections[collection_id] = page_statuses
            continue
        if not page_statuses[0].get('vernacular_filepath'):
            errored_collections[collection_id] = page_statuses
            continue
        version = get_version(
            collection_id,
            page_statuses[0]['vernacular_filepath']
        )
        print(
            "Review fetched data at: https://rikolti-data.s3.us-west-2."
            f"amazonaws.com/index.html#{version.rstrip('/')}/data/"
        )
        fetched_versions[collection_id] = version
    if errored_collections:
        print(
            f"{len(errored_collections)} encountered an error when "
            f"fetching: {errored_collections.keys()}"
        )
        print(errored_collections)

    if not fetched_versions:
        raise ValueError("No collections successfully fetched, exiting.")

    return fetched_versions

