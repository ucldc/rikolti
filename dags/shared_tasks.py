import boto3
import os
from datetime import datetime

import requests

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule

from urllib.parse import urlparse

from rikolti.metadata_fetcher.lambda_function import fetch_collection
from rikolti.metadata_mapper.lambda_function import map_page
from rikolti.metadata_mapper.lambda_shepherd import get_mapping_status
from rikolti.metadata_mapper.validate_mapping import create_collection_validation_csv
from rikolti.record_indexer.create_collection_index import create_new_index
from rikolti.record_indexer.create_collection_index import get_index_name
from rikolti.record_indexer.create_collection_index import delete_index
from rikolti.record_indexer.move_index_to_prod import move_index_to_prod
from rikolti.utils.versions import create_vernacular_version
from rikolti.utils.versions import get_version
from rikolti.utils.versions import create_mapped_version
from rikolti.utils.versions import create_content_data_version


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
    fetch_status = fetch_collection(collection, vernacular_version, {})
    success = all([page['status'] == 'success' for page in fetch_status])
    total_items = sum([page['document_count'] for page in fetch_status])
    total_pages = len(fetch_status)
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

    vernacular_filepaths = [page['vernacular_filepath'] for page in fetch_status]
    if not vernacular_filepaths or not success:
        raise Exception(
            'vernacular metadata not successfully fetched\n{fetch_status}')

    return vernacular_filepaths


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
def map_page_task(vernacular_page: str, collection: dict, mapped_data_version: str):
    """
    vernacular_page is a filepath relative to the collection id, ex:
        3433/vernacular_metadata_2023-01-01T00:00:00/data/1
    mapped_data_version is a path relative to the collection id, ex:
        3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/
    returns a dictionary with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path: str, ex: 
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    """
    collection_id = collection.get('id')
    if not collection_id or not mapped_data_version:
        return False
    mapped_page = map_page(
        collection_id, vernacular_page, mapped_data_version, collection)
    return mapped_page


@task()
def get_mapping_status_task(collection: dict, mapped_pages: list):
    """
    mapped_pages is a list of dicts with the following keys:
        status: success
        num_records_mapped: int
        page_exceptions: TODO
        mapped_page_path: str, ex: 
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl
    returns a dict with the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    mapping_status = get_mapping_status(collection, mapped_pages)
    return mapping_status


@task()
def create_mapped_version_task(collection, vernacular_pages):
    """
    vernacular pages is a list of the filepaths of the vernacular metadata
    relative to the collection id, ex: [
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/1',
        '3433/vernacular_metadata_2023-01-01T00:00:00/data/2'
    ]
    returns the path to a new mapped version, ex:
        "3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/"
    """
    vernacular_version = get_version(collection.get('id'), vernacular_pages[0])
    if not vernacular_version:
        raise ValueError(
            f"Vernacular version not found in {vernacular_pages[0]}")
    mapped_data_version = create_mapped_version(vernacular_version)
    return mapped_data_version


@task()
def validate_collection_task(collection_status: dict, params=None) -> str:
    """
    collection_status is a dict containing the following keys:
        mapped_page_paths: ex: [
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/1.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/2.jsonl,
            3433/vernacular_metadata_2023-01-01T00:00:00/mapped_metadata_2023-01-01T00:00:00/3.jsonl
        ]
    """
    if not params or not params.get('validate'):
        raise ValueError("Validate flag not found in params")

    # let this throw an error if no collection_id
    collection_id = int(collection_status['collection_id'])

    if collection_status.get('status') != 'success':
        raise Exception(f"Collection {collection_id} not successfully mapped")

    num_rows, file_location = create_collection_validation_csv(
        collection_id, collection_status['mapped_page_paths'])
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
def create_content_data_version_task(collection: dict, mapped_pages: list[dict]):
    mapped_version = get_version(
        collection['id'], mapped_pages[0]['mapped_page_path'])
    return create_content_data_version(mapped_version)


@task()
def create_stage_index_task(collection: dict, index_name: str):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    create_new_index(collection_id, index_name)


@task()
def get_index_name_task(collection: dict, version: str):
    collection_id = collection.get('id')
    if not collection_id:
        raise ValueError(
            f"Collection ID not found in collection metadata: {collection}")
    return get_index_name(collection_id, version)


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


