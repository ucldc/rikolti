import boto3
import pprint
import os
import math
from datetime import datetime
from typing import Union

import requests

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from urllib.parse import urlparse

from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.record_indexer.create_collection_index import create_new_index
from rikolti.record_indexer.create_collection_index import delete_index
from rikolti.record_indexer.move_index_to_prod import move_index_to_prod
from rikolti.utils.versions import create_vernacular_version
from rikolti.utils.versions import get_version
from rikolti.utils.versions import create_mapped_version
from rikolti.utils.versions import create_with_content_urls_version


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


def paginate_filepaths_for_fanout(filepaths):
    # 1024 is the maximum number of fanout tasks allowed
    page_size = math.ceil(len(filepaths) / 1024)
    paginated_filepaths = []
    for i in range(0, len(filepaths), page_size):
        paginated_filepaths.append(filepaths[i:i+page_size])
    return paginated_filepaths


@task()
def create_vernacular_version_task(collection) -> str:
    # returns: '3433/vernacular_metadata_v1/'
    return create_vernacular_version(collection.get('collection_id'))


@task()
def fetch_collection_task(collection: dict, vernacular_version: str):
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

    return paginate_filepaths_for_fanout(stats['filepaths'])


@task(multiple_outputs=True)
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
def map_page_task(
    vernacular_pages: Union[str,list[str]],
    collection: dict,
    mapped_data_version: str):
    """
    vernacular_pages is a list of filepaths relative to the collection id, ex:
        [ 3433/vernacular_metadata_2023-01-01T00:00:00/data/1 ]
    or:
        [
            3433/vernacular_metadata_2023-01-01T00:00:00/data/1,
            3433/vernacular_metadata_2023-01-01T00:00:00/data/2
        ]
    mapped_data_version is a path relative to the collection id, ex:
        3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/
    returns a dictionary with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path|None: str, ex:
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    """
    collection_id = collection.get('id')
    if not collection_id or not mapped_data_version:
        return False

    mapped_pages = []
    for vernacular_page in vernacular_pages:
        mapped_page = map_page(
            collection_id, vernacular_page, mapped_data_version, collection)
        mapped_pages.append(mapped_page)
    return mapped_pages


@task(multiple_outputs=True)
def get_mapping_status_task(collection: dict, paginated_mapped_pages: list):
    """
    mapped_pages is a list of a list of dicts with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path: str|None, ex:
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    returns a dict with the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    mapped_pages = []
    for pages in paginated_mapped_pages:
        mapped_pages.extend(pages)
    mapping_status = get_mapping_status(collection, mapped_pages)

    return mapping_status


@task()
def create_mapped_version_task(collection, vernacular_pages):
    """
    vernacular pages is a list of lists of the filepaths of the vernacular
    metadata relative to the collection id, ex: [
        ['3433/vernacular_metadata_2023-01-01T00:00:00/data/1'],
        ['3433/vernacular_metadata_2023-01-01T00:00:00/data/2]'
    ]
    returns the path to a new mapped version, ex:
        "3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/"
    """
    vernacular_version = get_version(collection.get('id'), vernacular_pages[0][0])
    if not vernacular_version:
        raise ValueError(
            f"Vernacular version not found in {vernacular_pages[0][0]}")
    mapped_data_version = create_mapped_version(vernacular_version)
    return mapped_data_version


@task()
def validate_collection_task(collection_id: int, mapped_metadata_pages: dict) -> str:
    """
    collection_status is a dict containing the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    parent_pages = [path for path in mapped_metadata_pages if 'children' not in path]

    num_rows, file_location = create_collection_validation_csv(
        collection_id, parent_pages)
    print(f"Output {num_rows} rows to {file_location}")

    # create a link to the file in the logs
    mapper_data_dest = os.environ.get("MAPPED_DATA", "file:///tmp")
    if mapper_data_dest.startswith("s3"):
        parsed_loc = urlparse(
            f"{mapper_data_dest.rstrip('/')}/{file_location}")
        file_location = (
            f"https://{parsed_loc.netloc}.s3.us-west-2."
            f"amazonaws.com{parsed_loc.path}"
        )

    return file_location


@task()
def create_with_content_urls_version_task(collection: dict, mapped_pages: list[list[dict]]):
    mapped_page_path = [page['mapped_page_path'] for page in mapped_pages[0]
        if page['mapped_page_path']][0]
    mapped_version = get_version(collection['id'], mapped_page_path)
    return create_with_content_urls_version(mapped_version)


@task()
def create_stage_index_task(collection: dict, version_pages: list[str]):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")

    index_name = create_new_index(collection_id, version_pages)
    return index_name


# Task is triggered if at least one upstream (direct parent) task has failed
@task(trigger_rule=TriggerRule.ONE_FAILED)
def cleanup_failed_index_creation_task(index_name: str):
    delete_index(index_name)


@task()
def move_index_to_prod_task(collection: dict):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    move_index_to_prod(collection_id)


@task()
def s3_to_localfilesystem(s3_url=None, params=None):
    """
    Download all files at a specified s3 location to the local filesystem.
    Requires an s3_url specified as an argument, or an s3_url in the DAG run
    parameters. The s3_url should be specified in s3://<bucket>/<prefix>
    format.
    """
    if not s3_url:
        s3_url = params.get('s3_url', None) if params else None
        if not s3_url:
            raise ValueError("No s3_url specified in params or as argument")

    # parse s3_url
    s3_url = urlparse(s3_url)
    if s3_url.scheme != 's3':
        raise ValueError(
            "s3_url must be specified in s3://<bucket>/<prefix> format")
    bucket = s3_url.netloc
    path = s3_url.path[1:]

    # query s3 for a list of files filtered by path
    s3_client = boto3.client('s3')
    keys = s3_client.list_objects_v2(Bucket=bucket, Prefix=path)
    if keys['KeyCount'] == 0:
        raise ValueError(f"No files found at {s3_url}")

    paths = []
    for key in keys['Contents']:
        # get the contents of a single s3 file
        obj = s3_client.get_object(Bucket=bucket, Key=key['Key'])
        contents = obj['Body'].read().decode('utf-8')

        # create directory structure represented by s3 path in local filesystem
        path = key['Key'].split('/')
        path.insert(0, 'tmp')
        path = os.path.sep + os.path.sep.join(path)
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), exist_ok=True)

        # write contents of s3 file to local filesystem
        with open(path, 'wb') as sync_file:
            sync_file.write(contents.encode('utf-8'))
        paths.append(path)

    return paths


