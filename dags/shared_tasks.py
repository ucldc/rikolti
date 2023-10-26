import boto3
import os
from datetime import datetime

import requests
from docker.types import Mount

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.docker.operators.docker import DockerOperator
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
            os.makedirs(os.path.dirname(path))

        # write contents of s3 file to local filesystem
        with open(path, 'wb') as sync_file:
            sync_file.write(contents.encode('utf-8'))
        paths.append(path)

    return paths


class ContentHarvestDockerOperator(DockerOperator):
    def __init__(self, collection_id, page, **kwargs):
        mounts = []
        if os.environ.get("CONTENT_DATA_MOUNT"):
            mounts.append(Mount(
                source=os.environ.get("CONTENT_DATA_MOUNT"),
                target="/rikolti_data",
                type="bind",
            ))
        if os.environ.get("CONTENT_MOUNT"):
            mounts.append(Mount(
                source=os.environ.get("CONTENT_MOUNT"),
                target="/rikolti_content",
                type="bind",
            ))
        if not mounts:
            mounts=None

        container_image = Variable.get(
            'content_harvester_image',
            default_var='public.ecr.aws/b6c7x7s4/rikolti/content_harvester'
        )
        container_version = Variable.get(
            'content_harvester_version',
            default_var='latest'
        )
        args = {
            "image": f"{container_image}:{container_version}",
            "container_name": f"content_harvester_{collection_id}_{page}",
            "command": [f"{collection_id}", f"{page}"],
            "network_mode": "bridge",
            "auto_remove": 'force',
            "mounts": mounts,
            "mount_tmp_dir": False,
            "environment": {
                "CONTENT_DATA_SRC": os.environ.get("CONTENT_DATA_SRC"),
                "CONTENT_DATA_DEST": os.environ.get("CONTENT_DATA_DEST"),
                "CONTENT_DEST": os.environ.get("CONTENT_DEST"),
                "NUXEO": os.environ.get("NUXEO"),
            },
        }
        args.update(kwargs)
        super().__init__(**args)
